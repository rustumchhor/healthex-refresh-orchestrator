import { beforeEach, describe, expect, it } from 'vitest';
import { claimJobs, releaseLeases } from '../src/domain/claim.js';
import { pool } from '../src/db.js';
import { expireLease, getJob, getTokens, insertJob, patientId, resetDb, setTokens } from './helpers.js';

/** CORE OPERATION 3 — "workers claim jobs atomically, respecting priority order". */
describe('claiming jobs', () => {
  beforeEach(async () => {
    await resetDb();
  });


  it('claims the highest priority job first', async () => {
    const pids = await Promise.all(['pat-0001', 'pat-0003', 'pat-0009'].map(patientId));
    await insertJob({ patientId: pids[0]!, priority: 100 });
    const urgent = await insertJob({ patientId: pids[1]!, priority: 900 });
    await insertJob({ patientId: pids[2]!, priority: 500 });

    const { claimed } = await claimJobs('worker-a', 1);
    expect(claimed[0]?.id).toBe(urgent.id);
  });

  it('marks a claimed job in_progress and leases it to the claiming worker', async () => {
    const pid = await patientId('pat-0001');
    const job = await insertJob({ patientId: pid });

    const { claimed } = await claimJobs('worker-a', 5);
    expect(claimed).toHaveLength(1);

    const stored = await getJob(job.id);
    expect(stored.status).toBe('in_progress');
    expect(stored.locked_by).toBe('worker-a');
    expect(stored.lease_expires_at!.getTime()).toBeGreaterThan(Date.now());
  });

  it('does not claim a job before its run_at', async () => {
    const pid = await patientId('pat-0001');
    await insertJob({ patientId: pid, runAtOffsetSeconds: 60 });

    const { claimed } = await claimJobs('worker-a', 5);
    expect(claimed).toHaveLength(0);
  });

  it('does not claim a job past its deadline', async () => {
    const pid = await patientId('pat-0001');
    await insertJob({ patientId: pid, deadlineOffsetSeconds: -1 });

    const { claimed } = await claimJobs('worker-a', 5);
    expect(claimed).toHaveLength(0);
  });

  /**
   * The central guarantee. Without SKIP LOCKED these two transactions would
   * either block on each other or hand the same row to both workers.
   */
  it('never hands the same job to two workers', async () => {
    const { rows } = await pool.query(`SELECT id FROM patients ORDER BY id LIMIT 20`);
    for (const r of rows) await insertJob({ patientId: r.id, priority: r.id });

    const [a, b] = await Promise.all([claimJobs('worker-a', 5), claimJobs('worker-b', 5)]);

    const idsA = a.claimed.map((j) => j.id);
    const idsB = b.claimed.map((j) => j.id);

    expect(idsA).toHaveLength(5);
    expect(idsB).toHaveLength(5);
    expect(idsA.filter((id) => idsB.includes(id))).toEqual([]);
    expect(new Set([...idsA, ...idsB]).size).toBe(10);
  });

  it('spreads a larger contended pool across many workers without overlap', async () => {
    const { rows } = await pool.query(`SELECT id FROM patients ORDER BY id LIMIT 40`);
    for (const r of rows) await insertJob({ patientId: r.id, priority: r.id });

    const results = await Promise.all(
      ['w1', 'w2', 'w3', 'w4', 'w5', 'w6'].map((w) => claimJobs(w, 5)),
    );

    const all = results.flatMap((r) => r.claimed.map((j) => j.id));
    expect(all.length).toBe(30);
    expect(new Set(all).size).toBe(30); // no double-processing
  });

  describe('recovering from an interrupted worker', () => {
    it('reclaims a job whose lease has expired', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });

      const first = await claimJobs('worker-a', 5);
      expect(first.claimed).toHaveLength(1);

      // worker-a dies here, without ever completing the job.
      await expireLease(job.id);

      const second = await claimJobs('worker-b', 5);
      expect(second.claimed.map((j) => j.id)).toEqual([job.id]);
      expect((await getJob(job.id)).locked_by).toBe('worker-b');
    });

    it('leaves a live lease alone', async () => {
      const pid = await patientId('pat-0001');
      await insertJob({ patientId: pid });

      await claimJobs('worker-a', 5);
      const second = await claimJobs('worker-b', 5);

      expect(second.claimed).toHaveLength(0);
    });

    it('hands leases back immediately on graceful shutdown', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });

      await claimJobs('worker-a', 5);
      const released = await releaseLeases('worker-a');
      expect(released).toBe(1);

      const second = await claimJobs('worker-b', 5);
      expect(second.claimed.map((j) => j.id)).toEqual([job.id]);
    });
  });

  describe('rate limit admission', () => {
    it('spends one token from every endpoint the patient uses', async () => {
      const pid = await patientId('pat-0002'); // epic + cerner
      await insertJob({ patientId: pid });

      const epicBefore = await getTokens('epic');
      const cernerBefore = await getTokens('cerner');

      await claimJobs('worker-a', 5);

      expect(await getTokens('epic')).toBeCloseTo(epicBefore - 1, 1);
      expect(await getTokens('cerner')).toBeCloseTo(cernerBefore - 1, 1);
    });

    it('does not dispatch when any one of the patient\'s endpoints is exhausted', async () => {
      const pid = await patientId('pat-0002'); // epic + cerner
      await insertJob({ patientId: pid });

      await setTokens('cerner', 0); // epic still has plenty

      const result = await claimJobs('worker-a', 5);
      expect(result.claimed).toHaveLength(0);
      expect(result.deferredForRateLimit).toBe(1);
    });

    /** A patient is only as fast as their slowest source. */
    it('still dispatches patients whose endpoints all have budget', async () => {
      const single = await patientId('pat-0001'); // epic only
      const multi = await patientId('pat-0002'); // epic + cerner
      await insertJob({ patientId: single, priority: 100 });
      await insertJob({ patientId: multi, priority: 100 });

      await setTokens('cerner', 0);

      const { claimed, deferredForRateLimit } = await claimJobs('worker-a', 5);
      expect(claimed.map((j) => j.patient_id)).toEqual([single]);
      expect(deferredForRateLimit).toBe(1);
    });

    it('returns a deferred job to the queue rather than failing it', async () => {
      const pid = await patientId('pat-0002');
      const job = await insertJob({ patientId: pid });
      await setTokens('cerner', 0);

      await claimJobs('worker-a', 5);

      const stored = await getJob(job.id);
      expect(stored.status).toBe('pending');
      expect(stored.locked_by).toBeNull();
      expect(stored.attempts).toBe(0); // being throttled is not an attempt
      expect(stored.failure_class).toBe('rate_limited');
    });

    it('gives scarce budget to the highest priority patients', async () => {
      const { rows } = await pool.query(
        `SELECT p.id FROM patients p
           JOIN patient_endpoints pe ON pe.patient_id = p.id
           JOIN ehr_endpoints e ON e.id = pe.endpoint_id AND e.key = 'epic'
          WHERE NOT EXISTS (
                  SELECT 1 FROM patient_endpoints pe2
                    JOIN ehr_endpoints e2 ON e2.id = pe2.endpoint_id
                   WHERE pe2.patient_id = p.id AND e2.key <> 'epic')
          ORDER BY p.id LIMIT 5`,
      );
      // Epic-only patients, so exactly one bucket governs them.
      const ids = rows.map((r) => r.id as number);
      for (let i = 0; i < ids.length; i++) await insertJob({ patientId: ids[i]!, priority: (i + 1) * 100 });

      await setTokens('epic', 2);

      const { claimed, deferredForRateLimit } = await claimJobs('worker-a', 5);
      expect(claimed).toHaveLength(2);
      expect(deferredForRateLimit).toBe(3);
      // The two highest priorities won.
      expect(claimed.map((j) => j.priority)).toEqual([500, 400]);
    });

    it('refills the bucket over time rather than on a schedule', async () => {
      await setTokens('epic', 0);
      await pool.query(
        `UPDATE rate_limit_buckets b SET last_refill_at = now() - interval '30 seconds'
           FROM ehr_endpoints e WHERE e.id = b.endpoint_id AND e.key = 'epic'`,
      );

      const pid = await patientId('pat-0001'); // epic only
      await insertJob({ patientId: pid });

      // 30s at 100/min is 50 tokens, so this must go through.
      const { claimed } = await claimJobs('worker-a', 5);
      expect(claimed).toHaveLength(1);
    });
  });
});
