import { beforeEach, describe, expect, it } from 'vitest';
import { claimJobs } from '../src/domain/claim.js';
import { recordOutcome } from '../src/domain/complete.js';
import { EhrClient, parseRetryAfter } from '../src/ehr/client.js';
import Fastify from 'fastify';
import { assertSafeToReset } from '../src/migrate.js';
import { failExpiredJobs } from '../src/domain/complete.js';
import { config } from '../src/config.js';
import { buildApi } from '../src/api.js';
import { pool } from '../src/db.js';
import { getJob, insertJob, patientId, resetDb, studyId } from './helpers.js';

/**
 * Boundary and robustness regressions (BUG_AUDIT.md 5–13).
 *
 * Everything here is a failure at an edge the core tests never touch: the HTTP
 * transport, malformed input, missing reference data, and destructive tooling.
 */
describe('robustness at the boundaries', () => {
  beforeEach(async () => {
    await resetDb();
  });

  /**
   * BUG 5 — a timed-out POST is ambiguous, not failed.
   *
   * The idempotency key is (job id, attempts). Classifying a transport failure
   * as `transient` advanced `attempts`, which rotated the key, so the retry
   * looked like a brand-new request to the source and started a second paid
   * retrieval. The key exists precisely to make this case safe.
   */
  describe('an unanswered send', () => {
    it('is classified as ambiguous rather than as a failure', async () => {
      // Port 1 is not listening: the request cannot be answered.
      const client = new EhrClient('http://127.0.0.1:1', 300);
      const outcome = await client.startRefresh('pat-0001', 'job-1:0');
      expect(outcome.kind).toBe('ambiguous');
    });

    it('does not advance the attempt counter, so the retry reuses the same key', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });
      const claimed = (await claimJobs('worker-a', 1)).claimed[0]!;

      const keyBefore = `${claimed.id}:${claimed.attempts}`;
      await recordOutcome(claimed, { kind: 'ambiguous', message: 'timeout' });

      const stored = await getJob(job.id);
      expect(stored.attempts).toBe(0); // unchanged
      expect(stored.status).toBe('pending');
      expect(stored.locked_by).toBeNull();
      expect(`${stored.id}:${stored.attempts}`).toBe(keyBefore); // same key on retry
    });

    it('leaves the retry budget intact across repeated ambiguity', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid, maxAttempts: 3 });

      for (let i = 0; i < 4; i++) {
        await pool.query(`UPDATE refresh_jobs SET run_at = now(), lease_expires_at = NULL WHERE id=$1`, [job.id]);
        const claimed = (await claimJobs(`w${i}`, 1)).claimed[0]!;
        await recordOutcome(claimed, { kind: 'ambiguous', message: 'timeout' });
      }

      const stored = await getJob(job.id);
      expect(stored.attempts).toBe(0); // four unanswered sends cost no attempts
      expect(stored.status).toBe('pending'); // bounded by deadline_at, not by retries
    });
  });

  /**
   * BUG 6 — `[].every(...)` is `true`, so a patient with no endpoint was
   * admitted free of charge and could be reported successful. The source only
   * ever sees a patient reference, so it can answer "completed" for data no
   * system was ever asked for — and we would stamp enrollment clocks fresh.
   */
  describe('a patient with no EHR endpoint', () => {
    it('is failed rather than dispatched', async () => {
      const pid = await patientId('pat-0001');
      await pool.query(`DELETE FROM patient_endpoints WHERE patient_id=$1`, [pid]);
      const job = await insertJob({ patientId: pid });

      const { claimed } = await claimJobs('worker-a', 5);

      expect(claimed).toHaveLength(0); // never dispatched
      const stored = await getJob(job.id);
      expect(stored.status).toBe('failed');
      expect(stored.failure_class).toBe('permanent');
      expect(stored.last_error).toContain('no EHR endpoint');
    });

    it('never advances enrollment clocks', async () => {
      const daily = await studyId('cardiology-longitudinal');
      const pid = await patientId('pat-0001');
      await pool.query(`DELETE FROM patient_endpoints WHERE patient_id=$1`, [pid]);
      await insertJob({ patientId: pid, studyIds: [daily] });

      await claimJobs('worker-a', 5);

      const { rows } = await pool.query(
        `SELECT last_refresh_at FROM patient_studies WHERE patient_id=$1 AND study_id=$2`,
        [pid, daily],
      );
      expect(rows[0]?.last_refresh_at).toBeNull(); // data quality problem, not fresh data
    });

    it('does not consume rate-limit budget on the way out', async () => {
      const pid = await patientId('pat-0001');
      await pool.query(`DELETE FROM patient_endpoints WHERE patient_id=$1`, [pid]);
      await insertJob({ patientId: pid });

      const before = await pool.query(`SELECT sum(tokens)::float8 AS t FROM rate_limit_buckets`);
      await claimJobs('worker-a', 5);
      const after = await pool.query(`SELECT sum(tokens)::float8 AS t FROM rate_limit_buckets`);

      expect(after.rows[0]?.t).toBeCloseTo(before.rows[0]?.t, 0);
    });
  });

  /**
   * BUG 14 — a missing rate_limit_buckets row read as "zero tokens", which is
   * indistinguishable from being throttled. Every patient on that endpoint
   * deferred forever and nothing anywhere reported a problem.
   */
  it('self-heals an endpoint whose rate-limit bucket is missing', async () => {
    await pool.query(
      `DELETE FROM rate_limit_buckets b USING ehr_endpoints e
        WHERE e.id = b.endpoint_id AND e.key = 'cerner'`,
    );
    const pid = await patientId('pat-0002'); // epic + cerner
    await insertJob({ patientId: pid });

    const { claimed, deferredForRateLimit } = await claimJobs('worker-a', 5);

    expect(claimed).toHaveLength(1); // dispatched, not silently stuck
    expect(deferredForRateLimit).toBe(0);

    const { rows } = await pool.query(
      `SELECT b.capacity FROM rate_limit_buckets b
         JOIN ehr_endpoints e ON e.id = b.endpoint_id WHERE e.key='cerner'`,
    );
    expect(rows[0]?.capacity).toBe(30); // rebuilt from the endpoint's declared limit
  });

  /** BUG 7 — malformed input must be rejected at the edge, not by PostgreSQL. */
  describe('malformed API input', () => {
    it('rejects a non-numeric patient id with 400, not 500', async () => {
      const app = buildApi();
      const res = await app.inject({ method: 'GET', url: '/patients/not-a-number/studies/1/eligibility' });

      expect(res.statusCode).toBe(400);
      expect(res.body).not.toContain('22P02'); // no SQLSTATE leaked
      await app.close();
    });

    it('rejects a non-numeric limit with 400, not 500', async () => {
      const app = buildApi();
      const res = await app.inject({ method: 'GET', url: '/jobs?limit=not-a-number' });

      expect(res.statusCode).toBe(400);
      expect(res.body).not.toContain('22P02');
      await app.close();
    });

    it('rejects an out-of-range limit', async () => {
      const app = buildApi();
      expect((await app.inject({ method: 'GET', url: '/jobs?limit=99999' })).statusCode).toBe(400);
      expect((await app.inject({ method: 'GET', url: '/jobs?limit=0' })).statusCode).toBe(400);
      await app.close();
    });

    it('still serves valid requests', async () => {
      const app = buildApi();
      expect((await app.inject({ method: 'GET', url: '/jobs?limit=10' })).statusCode).toBe(200);
      expect((await app.inject({ method: 'GET', url: '/ready' })).statusCode).toBe(200);
      await app.close();
    });
  });

  /** BUG 8 — RFC 9110 allows an absolute date, which used to parse as 1 second. */
  describe('Retry-After parsing', () => {
    const now = Date.parse('2026-08-16T20:00:00Z');

    it('handles delay-seconds', () => {
      expect(parseRetryAfter('120', 3600, now)).toBe(120);
    });

    it('handles an HTTP-date in the future', () => {
      expect(parseRetryAfter('Sun, 16 Aug 2026 21:00:00 GMT', 3600, now)).toBe(3600);
    });

    it('treats a past date as retry now', () => {
      expect(parseRetryAfter('Sun, 16 Aug 2026 19:00:00 GMT', 3600, now)).toBe(1);
    });

    it('clamps an absurd value', () => {
      expect(parseRetryAfter('999999999', 3600, now)).toBe(3600);
    });

    it('falls back safely on garbage or absence', () => {
      expect(parseRetryAfter('not-a-date', 3600, now)).toBe(1);
      expect(parseRetryAfter(null, 3600, now)).toBe(1);
    });
  });

  /**
   * BUG 17 — an accepted send whose body we cannot read is still an accepted
   * send. Returning `transient` advanced `attempts`, rotated the idempotency
   * key, and bought a second retrieval. Same class as BUG 5, one branch lower.
   */
  it('treats a 202 with an unreadable body as ambiguous, not failed', async () => {
    const app = Fastify();
    app.post('/patients/:id/$updateData', async (_req, reply) => {
      reply.code(202); // accepted, but no requestId in the payload
      return {};
    });
    await app.listen({ port: 0, host: '127.0.0.1' });
    const port = (app.server.address() as { port: number }).port;

    const outcome = await new EhrClient(`http://127.0.0.1:${port}`, 1_000).startRefresh('pat-0001', '1:0');
    expect(outcome.kind).toBe('ambiguous'); // never 'transient'
    await app.close();
  });

  /**
   * BUG 18 — a 404 while polling is not proof of absence. A distributed source
   * can 404 a retrieval it has accepted but not yet replicated; restarting
   * under a new key turns replication lag into duplicate paid work.
   */
  it('retries the poll on 404 instead of restarting the refresh', async () => {
    const app = Fastify();
    app.get('/patients/:id/data-retrieval/status', async (_req, reply) => {
      reply.code(404);
      return { error: 'not_found' };
    });
    await app.listen({ port: 0, host: '127.0.0.1' });
    const port = (app.server.address() as { port: number }).port;

    const outcome = await new EhrClient(`http://127.0.0.1:${port}`, 1_000).pollStatus('pat-0001', 'req-1');
    expect(outcome.kind).toBe('poll_deferred'); // keeps the request id
    await app.close();
  });

  /** BUG 19 — ambiguity needs its own exponential counter and a hard ceiling. */
  describe('bounding ambiguous retries', () => {
    it('backs off exponentially in its own counter, not in attempts', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });
      const delays: number[] = [];

      for (let i = 0; i < 3; i++) {
        await pool.query(`UPDATE refresh_jobs SET run_at = now(), lease_expires_at = NULL WHERE id=$1`, [job.id]);
        const claimed = (await claimJobs(`w${i}`, 1)).claimed[0]!;
        const before = Date.now();
        await recordOutcome(claimed, { kind: 'ambiguous', message: 'timeout' });
        const stored = await getJob(job.id);
        delays.push(stored.run_at.getTime() - before);
        expect(stored.ambiguous_attempts).toBe(i + 1);
        expect(stored.attempts).toBe(0); // key never rotates
      }

      expect(delays[2]!).toBeGreaterThan(delays[0]!); // exponential, not fixed
    });

    it('terminates on an absolute lifetime that nothing can extend', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });

      // deadline_at is deliberately extendable; created_at is not.
      await pool.query(
        `UPDATE refresh_jobs
            SET created_at  = now() - make_interval(secs => $2::float8 + 1),
                deadline_at = now() + interval '1 hour'
          WHERE id = $1`,
        [job.id, config.MAX_JOB_LIFETIME_SECONDS],
      );

      expect(await failExpiredJobs()).toBe(1);
      expect((await getJob(job.id)).failure_class).toBe('timeout');
    });
  });

  /** BUG 20 — a readiness probe is often unauthenticated; it must stay quiet. */
  it('does not narrate database internals from /ready', async () => {
    const app = buildApi();
    const res = await app.inject({ method: 'GET', url: '/ready' });
    expect(res.body).not.toMatch(/postgres|SQLSTATE|ECONNREFUSED|password/i);
    await app.close();
  });

  /** BUG 13 — resetDatabase drops every table; it must not trust the URL. */
  describe('destructive-operation guard', () => {
    it('allows a local test database', () => {
      expect(() => assertSafeToReset('postgres://u@localhost:5432/healthex_test')).not.toThrow();
    });

    it('refuses a remote host', () => {
      expect(() => assertSafeToReset('postgres://u@prod-db.internal:5432/healthex')).toThrow(/Refusing to reset/);
    });

    it('refuses a database that does not look disposable', () => {
      expect(() => assertSafeToReset('postgres://u@localhost:5432/patients_live')).toThrow(/Refusing to reset/);
    });

    /** BUG 21 — the primary local database is still a database. */
    it('refuses the primary local database without an explicit opt-in', () => {
      expect(() => assertSafeToReset('postgres://u@localhost:5432/healthex')).toThrow(/Refusing to reset/);
    });
  });
});
