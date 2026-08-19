import { beforeEach, describe, expect, it } from 'vitest';
import { claimJobs } from '../src/domain/claim.js';
import { computeBackoffSeconds, failExpiredJobs, recordOutcome } from '../src/domain/complete.js';
import { pool } from '../src/db.js';
import { getJob, insertJob, patientId, resetDb, setLastRefresh, setTokens, studyId } from './helpers.js';
import type { ClaimedJob } from '../src/types.js';

/** CORE OPERATION 4 — "record success or failure, schedule retry with backoff if transient". */
describe('completing jobs', () => {
  beforeEach(async () => {
    await resetDb();
  });


  /**
   * A real second claim, as the worker loop performs it. Faking one by spreading
   * a refetched row over the old ClaimedJob no longer works — and should not,
   * since completion writes are now fenced on the lease holder.
   */
  async function reclaim(worker = 'worker-b'): Promise<ClaimedJob> {
    await pool.query(`UPDATE refresh_jobs SET run_at = now()`); // its poll time has arrived
    const { claimed } = await claimJobs(worker, 1);
    if (!claimed[0]) throw new Error('expected to reclaim the job');
    return claimed[0];
  }

  async function claimOne(patientRef = 'pat-0001', studyIds: number[] = []): Promise<ClaimedJob> {
    const pid = await patientId(patientRef);
    await insertJob({ patientId: pid, studyIds });
    const { claimed } = await claimJobs('worker-a', 1);
    if (!claimed[0]) throw new Error('expected to claim a job');
    return claimed[0];
  }

  describe('asynchronous retrieval', () => {
    it('records the request id and releases the worker when accepted', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'accepted', requestId: 'req-123' });

      const stored = await getJob(job.id);
      expect(stored.status).toBe('in_progress'); // still ours, but nobody is holding it
      expect(stored.external_request_id).toBe('req-123');
      expect(stored.locked_by).toBeNull();
      expect(stored.lease_expires_at).toBeNull();
      expect(stored.attempts).toBe(1);
    });

    it('does not spend an attempt while the source is still working', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'accepted', requestId: 'req-123' });

      await recordOutcome(await reclaim(), { kind: 'still_running' });

      const stored = await getJob(job.id);
      expect(stored.attempts).toBe(1); // unchanged by polling
      expect(stored.external_request_id).toBe('req-123');
      expect(stored.locked_by).toBeNull();
    });

    it('keeps the retrieval when only the poll failed', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'accepted', requestId: 'req-123' });
      await recordOutcome(await reclaim(), { kind: 'poll_deferred', message: 'gateway timeout' });

      const stored = await getJob(job.id);
      // We do not restart the refresh just because we failed to observe it.
      expect(stored.external_request_id).toBe('req-123');
      expect(stored.attempts).toBe(1);
    });
  });

  describe('success', () => {
    it('marks the job completed', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'success' });

      const stored = await getJob(job.id);
      expect(stored.status).toBe('completed');
      expect(stored.finished_at).not.toBeNull();
      expect(stored.last_error).toBeNull();
      expect(stored.failure_class).toBeNull();
    });

    /**
     * Transaction consistency: the status change and the enrollment clocks are
     * one atomic act. If they could diverge we would either pay for a refresh
     * twice or claim data is fresh when we never fetched it.
     */
    it('advances every active enrollment, atomically', async () => {
      const daily = await studyId('cardiology-longitudinal');
      const acute = await studyId('acute-monitoring');
      const job = await claimOne('pat-0005', [daily, acute]);

      await recordOutcome(job, { kind: 'success' });

      const { rows } = await pool.query(
        `SELECT study_id, last_refresh_at FROM patient_studies WHERE patient_id=$1 ORDER BY study_id`,
        [job.patient_id],
      );
      expect(rows.every((r) => r.last_refresh_at !== null)).toBe(true);
      expect((await getJob(job.id)).status).toBe('completed');
    });

    /**
     * The distinction this whole design turns on: `coalesced_study_ids` records
     * the DUE studies that triggered the refresh; the refresh itself satisfies
     * every ACTIVE association, because one patient-level call refreshes the
     * patient's data outright. HealthEx confirmed this reading.
     *
     * pat-0005 is enrolled in the daily study (never refreshed, therefore due)
     * and in acute-monitoring (refreshed a second ago, therefore NOT due). Only
     * the daily study is on the job; both clocks must move.
     */
    it('advances active associations that were not due, not only the ones that triggered it', async () => {
      const pid = await patientId('pat-0005');
      const daily = await studyId('cardiology-longitudinal');
      const acute = await studyId('acute-monitoring');

      await setLastRefresh(pid, daily, null); // due: never refreshed
      await setLastRefresh(pid, acute, '1 second'); // active, comfortably inside its 2-minute interval

      const { rows: before } = await pool.query(
        `SELECT last_refresh_at FROM patient_studies WHERE patient_id=$1 AND study_id=$2`,
        [pid, acute],
      );
      const acuteBefore = before[0]?.last_refresh_at as Date;

      const job = await claimOne('pat-0005', [daily]); // acute was not due, so it is not on the job
      await recordOutcome(job, { kind: 'success' });

      const { rows } = await pool.query(
        `SELECT study_id, last_refresh_at FROM patient_studies WHERE patient_id=$1 ORDER BY study_id`,
        [pid],
      );
      expect(rows).toHaveLength(2);
      expect(rows.every((r) => r.last_refresh_at !== null)).toBe(true);

      const acuteAfter = rows.find((r) => r.study_id === acute)?.last_refresh_at as Date;
      expect(acuteAfter.getTime()).toBeGreaterThan(acuteBefore.getTime());

      // The job still records only what was due. It is an audit of the trigger,
      // not the set of associations the refresh satisfied.
      expect((await getJob(job.id)).coalesced_study_ids).toEqual([daily]);
    });

    /**
     * A withdrawn enrollment has no claim on this data. Marking it fresh would
     * imply we are still refreshing on its behalf, which we are not.
     */
    it('does not advance a withdrawn association', async () => {
      const pid = await patientId('pat-0005');
      const daily = await studyId('cardiology-longitudinal');
      const acute = await studyId('acute-monitoring');
      await pool.query(
        `UPDATE patient_studies SET status='withdrawn', last_refresh_at=NULL
          WHERE patient_id=$1 AND study_id=$2`,
        [pid, acute],
      );

      const job = await claimOne('pat-0005', [daily]);
      await recordOutcome(job, { kind: 'success' });

      const { rows } = await pool.query(
        `SELECT study_id, status, last_refresh_at FROM patient_studies WHERE patient_id=$1`,
        [pid],
      );
      expect(rows.find((r) => r.study_id === acute)?.last_refresh_at).toBeNull();
      expect(rows.find((r) => r.study_id === daily)?.last_refresh_at).not.toBeNull();
    });
  });

  describe('failure handling', () => {
    it('retries a transient failure with backoff', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'transient', message: 'upstream 503' });

      const stored = await getJob(job.id);
      expect(stored.status).toBe('pending');
      expect(stored.attempts).toBe(1);
      expect(stored.failure_class).toBe('transient');
      expect(stored.last_error).toContain('503');
      expect(stored.run_at.getTime()).toBeGreaterThan(Date.now()); // deferred, not immediate
    });

    it('restarts a transient retry from $updateData, not from a poll', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'accepted', requestId: 'req-123' });
      await recordOutcome(await reclaim(), { kind: 'transient', message: 'retrieval failed' });

      // A stale request id would make the next attempt poll a dead retrieval.
      expect((await getJob(job.id)).external_request_id).toBeNull();
    });

    it('gives up once the retry budget is exhausted', async () => {
      const pid = await patientId('pat-0001');
      await insertJob({ patientId: pid, maxAttempts: 2 });

      for (let i = 0; i < 2; i++) {
        await pool.query(`UPDATE refresh_jobs SET run_at = now(), lease_expires_at = NULL WHERE patient_id=$1`, [pid]);
        const { claimed } = await claimJobs(`worker-${i}`, 1);
        await recordOutcome(claimed[0]!, { kind: 'transient', message: 'upstream 503' });
      }

      const { rows } = await pool.query(`SELECT * FROM refresh_jobs WHERE patient_id=$1`, [pid]);
      expect(rows[0]?.status).toBe('failed');
      expect(rows[0]?.attempts).toBe(2);
      expect(rows[0]?.failure_class).toBe('transient');
      expect(rows[0]?.finished_at).not.toBeNull();
    });

    it('never retries a permanent failure', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'permanent', message: 'consent revoked' });

      const stored = await getJob(job.id);
      expect(stored.status).toBe('failed');
      expect(stored.failure_class).toBe('permanent');
      expect(stored.finished_at).not.toBeNull();
      // Terminal on the first attempt, with retries still available.
      expect(stored.attempts).toBeLessThan(stored.max_attempts);
    });

    /** Being throttled says nothing about this job's chance of succeeding. */
    it('does not spend a retry attempt on being rate limited', async () => {
      const job = await claimOne();
      await recordOutcome(job, { kind: 'rate_limited', retryAfterSeconds: 1, message: 'throttled' });

      const stored = await getJob(job.id);
      expect(stored.status).toBe('pending');
      expect(stored.attempts).toBe(0);
      expect(stored.failure_class).toBe('rate_limited');
      expect(stored.run_at.getTime()).toBeGreaterThan(Date.now());
    });
  });

  /**
   * Regression: a worker whose lease lapsed could still write.
   *
   * Lease expiry makes a job *reclaimable*; it does nothing to stop the original
   * worker from waking up afterwards and overwriting the replacement's work.
   * Observed before the fix: worker B recorded a live external_request_id, then
   * a stale worker A cleared it and double-counted attempts — the job restarted
   * from scratch while a real retrieval was still running at the source.
   */
  describe('fencing against a worker that lost its lease', () => {
    it('refuses a write from a worker whose lease was taken', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });

      const stale = (await claimJobs('worker-a', 1)).claimed[0]!;
      await pool.query(`UPDATE refresh_jobs SET lease_expires_at = now() - interval '1 second' WHERE id=$1`, [job.id]);
      const fresh = (await claimJobs('worker-b', 1)).claimed[0]!;

      expect(await recordOutcome(fresh, { kind: 'accepted', requestId: 'req-B' })).toBe(true);
      expect(await recordOutcome(stale, { kind: 'transient', message: 'zombie' })).toBe(false);

      const stored = await getJob(job.id);
      expect(stored.external_request_id).toBe('req-B'); // live work survived
      expect(stored.attempts).toBe(1); // not double counted
      expect(stored.status).toBe('in_progress');
    });

    it('does not advance enrollment clocks on a refused success', async () => {
      const daily = await studyId('cardiology-longitudinal');
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid, studyIds: [daily] });

      const stale = (await claimJobs('worker-a', 1)).claimed[0]!;
      await pool.query(`UPDATE refresh_jobs SET lease_expires_at = now() - interval '1 second' WHERE id=$1`, [job.id]);
      await claimJobs('worker-b', 1);

      expect(await recordOutcome(stale, { kind: 'success' })).toBe(false);

      const { rows } = await pool.query(
        `SELECT last_refresh_at FROM patient_studies WHERE patient_id=$1 AND study_id=$2`, [pid, daily]);
      expect(rows[0]?.last_refresh_at).toBeNull(); // never fetched, never marked fresh
    });
  });

  /**
   * Regression: waiting for rate-limit budget used to consume the execution
   * deadline, so a starved job was eventually failed as a 'timeout' having never
   * been permitted to send a single request.
   */
  describe('throttling versus the execution deadline', () => {
    it('extends the deadline while a job waits for budget', async () => {
      const pid = await patientId('pat-0002'); // epic + cerner
      const job = await insertJob({ patientId: pid });
      const before = (await getJob(job.id)).deadline_at;

      await setTokens('cerner', 0);
      for (let i = 0; i < 3; i++) await claimJobs('worker-a', 5);

      const after = await getJob(job.id);
      expect(after.deadline_at.getTime()).toBeGreaterThan(before.getTime());
      expect(after.attempts).toBe(0); // still never dispatched
      expect(after.status).toBe('pending'); // queued, not failed
    });
  });

  describe('deadlines', () => {
    it('fails jobs that outran their deadline', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid, deadlineOffsetSeconds: -1 });

      expect(await failExpiredJobs()).toBe(1);

      const stored = await getJob(job.id);
      expect(stored.status).toBe('failed');
      expect(stored.failure_class).toBe('timeout');
    });

    it('leaves live jobs alone', async () => {
      const pid = await patientId('pat-0001');
      await insertJob({ patientId: pid, deadlineOffsetSeconds: 300 });
      expect(await failExpiredJobs()).toBe(0);
    });
  });
});

describe('retry backoff', () => {
  it('grows exponentially', () => {
    const delays = [1, 2, 3, 4].map((a) => computeBackoffSeconds(a, 7, 1, 1_000));
    for (let i = 1; i < delays.length; i++) {
      expect(delays[i]!).toBeGreaterThan(delays[i - 1]!);
    }
  });

  it('is capped', () => {
    expect(computeBackoffSeconds(30, 7, 1, 60)).toBeLessThanOrEqual(60);
  });

  it('is reproducible for the same job and attempt', () => {
    expect(computeBackoffSeconds(3, 42, 1, 1_000)).toBe(computeBackoffSeconds(3, 42, 1, 1_000));
  });

  /** Correlated failures must not produce a synchronised retry storm. */
  it('separates jobs that failed at the same instant', () => {
    const delays = new Set(Array.from({ length: 100 }, (_, id) => computeBackoffSeconds(2, id, 10, 1_000)));
    expect(delays.size).toBeGreaterThan(90);
  });
});
