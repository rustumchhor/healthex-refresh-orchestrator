import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Worker } from '../src/worker.js';
import { Scheduler } from '../src/scheduler.js';
import { pool } from '../src/db.js';
import {
  getJob,
  getTokens,
  insertJob,
  jobForPatient,
  patientId,
  resetDb,
  sleep,
  startMockEhr,
  studyId,
  type MockHarness,
} from './helpers.js';
import type { RefreshJob } from '../src/types.js';

/**
 * End-to-end: a real worker, a real database, and the mock EHR over real HTTP.
 *
 * These exercise the asynchronous shape of the external API — $updateData
 * returns immediately and the retrieval finishes later — which is the part that
 * makes the worker release the job between polls.
 */
describe('worker end to end', () => {
  let mock: MockHarness;

  beforeEach(async () => {
    await resetDb();
    mock = await startMockEhr();
  });

  afterEach(async () => {
    await mock?.close();
  });


  /** Run claim-and-process cycles until the job stops moving. */
  async function drive(worker: Worker, jobId: number, maxCycles = 100): Promise<RefreshJob> {
    for (let i = 0; i < maxCycles; i++) {
      await worker.runOnce();
      const job = await getJob(jobId);
      if (job.status === 'completed' || job.status === 'failed') return job;
      await sleep(40);
    }
    return getJob(jobId);
  }

  it('drives a patient through $updateData, polling, and completion', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', { kind: 'success', retrievalMs: 0 });
    const job = await insertJob({ patientId: pid });

    const worker = new Worker('worker-e2e', mock.client);
    const final = await drive(worker, job.id);

    expect(final.status).toBe('completed');
    expect(final.external_request_id).not.toBeNull();
    expect(final.attempts).toBe(1);
  });

  it('waits for a slow retrieval instead of failing it', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', { kind: 'success', retrievalMs: 400 });
    const job = await insertJob({ patientId: pid });

    const worker = new Worker('worker-e2e', mock.client);

    // First cycle only starts the retrieval; it cannot already be done.
    await worker.runOnce();
    const afterStart = await getJob(job.id);
    expect(afterStart.status).toBe('in_progress');
    expect(afterStart.external_request_id).not.toBeNull();
    // Crucially, no worker is holding it while the source works.
    expect(afterStart.locked_by).toBeNull();

    expect((await drive(worker, job.id)).status).toBe('completed');
  });

  it('recovers from a transient failure and eventually succeeds', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', {
      kind: 'transient',
      failAt: 'post',
      transientFailures: 1,
      retrievalMs: 0,
    });
    const job = await insertJob({ patientId: pid, maxAttempts: 3 });

    const final = await drive(new Worker('worker-e2e', mock.client), job.id);

    expect(final.status).toBe('completed');
    expect(final.attempts).toBe(2); // one failure, then success
  });

  it('recovers when the failure surfaces during retrieval rather than on POST', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', {
      kind: 'transient',
      failAt: 'retrieval',
      transientFailures: 1,
      retrievalMs: 0,
    });
    const job = await insertJob({ patientId: pid, maxAttempts: 3 });

    const final = await drive(new Worker('worker-e2e', mock.client), job.id);
    expect(final.status).toBe('completed');
  });

  it('fails a permanent error immediately, without retrying', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', { kind: 'permanent', failAt: 'post' });
    const job = await insertJob({ patientId: pid, maxAttempts: 3 });

    const final = await drive(new Worker('worker-e2e', mock.client), job.id);

    expect(final.status).toBe('failed');
    expect(final.failure_class).toBe('permanent');
    expect(final.attempts).toBe(1); // one call, no money wasted on retries
  });

  it('gives up on a persistently failing source once the budget is spent', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', {
      kind: 'transient',
      failAt: 'post',
      transientFailures: 99, // never recovers
    });
    const job = await insertJob({ patientId: pid, maxAttempts: 2 });

    const final = await drive(new Worker('worker-e2e', mock.client), job.id);

    expect(final.status).toBe('failed');
    expect(final.failure_class).toBe('transient');
    expect(final.attempts).toBe(2);
  });

  it('backs off without burning retries when the source throttles us', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', { kind: 'rate_limited' });
    const job = await insertJob({ patientId: pid, maxAttempts: 3 });

    await new Worker('worker-e2e', mock.client).runOnce();

    const stored = await getJob(job.id);
    expect(stored.status).toBe('pending');
    expect(stored.failure_class).toBe('rate_limited');
    expect(stored.attempts).toBe(0);
  });

  it('processes many patients across several workers without double-processing', async () => {
    const { rows } = await pool.query(`SELECT id, external_ref FROM patients ORDER BY id LIMIT 12`);
    for (const r of rows) {
      await mock.setBehavior(r.external_ref, { kind: 'success', retrievalMs: 0 });
      await insertJob({ patientId: r.id, priority: r.id });
    }

    const workers = [1, 2, 3].map((i) => new Worker(`worker-${i}`, mock.client));

    for (let cycle = 0; cycle < 40; cycle++) {
      await Promise.all(workers.map((w) => w.runOnce()));
      const { rows: done } = await pool.query(
        `SELECT count(*)::int AS n FROM refresh_jobs WHERE status IN ('completed','failed')`,
      );
      if (done[0]?.n === 12) break;
      await sleep(40);
    }

    const { rows: summary } = await pool.query(
      `SELECT count(*) FILTER (WHERE status='completed')::int AS completed,
              count(*)::int                                   AS total,
              max(attempts)::int                              AS max_attempts
         FROM refresh_jobs`,
    );
    expect(summary[0]?.completed).toBe(12);
    expect(summary[0]?.total).toBe(12);
    // One successful refresh per patient — nobody was processed twice.
    expect(summary[0]?.max_attempts).toBe(1);
  });

  it('resets the enrollment clock so the patient is not immediately due again', async () => {
    const pid = await patientId('pat-0005');
    const daily = await studyId('cardiology-longitudinal');
    const acute = await studyId('acute-monitoring');
    await mock.setBehavior('pat-0005', { kind: 'success', retrievalMs: 0 });

    const job = await insertJob({ patientId: pid, studyIds: [daily, acute] });
    await drive(new Worker('worker-e2e', mock.client), job.id);

    const { rows } = await pool.query(
      `SELECT count(*) FILTER (WHERE last_refresh_at IS NOT NULL)::int AS refreshed
         FROM patient_studies WHERE patient_id=$1`,
      [pid],
    );
    expect(rows[0]?.refreshed).toBe(2);
  });
});

describe('scheduler and workers together', () => {
  let mock: MockHarness;

  beforeEach(async () => {
    await resetDb();
    mock = await startMockEhr();
  });

  afterEach(async () => {
    await mock?.close();
  });


  it('schedules due patients and drains them', async () => {
    const { rows } = await pool.query(`SELECT external_ref FROM patients`);
    for (const r of rows) await mock.setBehavior(r.external_ref, { kind: 'success', retrievalMs: 0 });

    // Lift the rate limits for this test only.
    //
    // The seeded limits are realistic and they bite hard: Cerner allows 30/min,
    // 30 of the 60 patients depend on it, and every job costs two calls (the
    // POST plus at least one status poll). Draining all 60 therefore takes
    // roughly a minute — correct behaviour, asserted in the test below. This
    // test is about scheduler/worker integration, so throughput must not be the
    // thing being measured.
    await pool.query(`UPDATE rate_limit_buckets SET tokens = 100000, capacity = 100000, refill_per_sec = 10000`);

    const scheduled = await new Scheduler().tick();
    expect(scheduled.scheduled).toBe(60);

    const workers = [1, 2, 3, 4].map((i) => new Worker(`worker-${i}`, mock.client));
    for (let cycle = 0; cycle < 120; cycle++) {
      await Promise.all(workers.map((w) => w.runOnce()));
      const { rows: left } = await pool.query(
        `SELECT count(*)::int AS n FROM refresh_jobs WHERE status IN ('pending','in_progress')`,
      );
      if (left[0]?.n === 0) break;
      await sleep(30);
    }

    const { rows: summary } = await pool.query(
      `SELECT status, count(*)::int AS n FROM refresh_jobs GROUP BY status`,
    );
    const completed = summary.find((s) => s.status === 'completed')?.n ?? 0;
    expect(completed).toBe(60);
  });

  /**
   * The converse, and the more interesting property.
   *
   * Under the seeded limits a full drain is impossible in a few seconds, so the
   * system has to degrade by *queueing* rather than by failing or by silently
   * dropping work. This is "a patient is only as fast as their slowest source"
   * observed end to end: patients served solely by Epic (100/min) finish, while
   * patients who also depend on Cerner (30/min) wait their turn.
   */
  it('degrades by queueing when an endpoint runs out of budget', async () => {
    const { rows } = await pool.query(`SELECT external_ref FROM patients`);
    for (const r of rows) await mock.setBehavior(r.external_ref, { kind: 'success', retrievalMs: 0 });

    await new Scheduler().tick();

    const workers = [1, 2, 3, 4].map((i) => new Worker(`worker-${i}`, mock.client));
    for (let cycle = 0; cycle < 25; cycle++) {
      await Promise.all(workers.map((w) => w.runOnce()));
      await sleep(20);
    }

    const { rows: summary } = await pool.query(
      `SELECT count(*)::int                                                   AS total,
              count(*) FILTER (WHERE status = 'completed')::int               AS completed,
              count(*) FILTER (WHERE status IN ('pending','in_progress'))::int AS queued,
              count(*) FILTER (WHERE status = 'failed')::int                  AS failed
         FROM refresh_jobs`,
    );
    const s = summary[0]!;

    // Nothing is lost: every scheduled job is still accounted for somewhere.
    expect(s.total).toBe(60);
    expect(s.completed + s.queued + s.failed).toBe(60);
    // Being throttled is not a failure.
    expect(s.failed).toBe(0);
    // Real progress was made, and the slow endpoint held the rest back.
    expect(s.completed).toBeGreaterThan(0);
    expect(s.queued).toBeGreaterThan(0);
    // Direct evidence of which endpoint was the constraint.
    const cerner = await getTokens('cerner');
    expect(cerner).toBeLessThan(1);
    expect(await getTokens('epic')).toBeGreaterThan(cerner);
  });

  it('does not reschedule a patient whose refresh just succeeded', async () => {
    const pid = await patientId('pat-0001');
    await mock.setBehavior('pat-0001', { kind: 'success', retrievalMs: 0 });

    await new Scheduler().tick();
    const job = await jobForPatient(pid);

    const worker = new Worker('worker-e2e', mock.client);
    for (let i = 0; i < 40; i++) {
      await worker.runOnce();
      if ((await getJob(job!.id)).status === 'completed') break;
      await sleep(30);
    }

    await new Scheduler().tick();

    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM refresh_jobs WHERE patient_id=$1`, [pid]);
    expect(rows[0]?.n).toBe(1);
  });
});
