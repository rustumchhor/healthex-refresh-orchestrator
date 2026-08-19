import { beforeEach, describe, expect, it } from 'vitest';
import { scheduleRefreshBatch } from '../src/domain/schedule.js';
import { jitterSeconds } from '../src/domain/jitter.js';
import { failExpiredJobs } from '../src/domain/complete.js';
import { config } from '../src/config.js';
import { pool } from '../src/db.js';
import { jobForPatient, patientId, resetDb, studyId } from './helpers.js';

/** CORE OPERATION 2 — "accept a list of patients, create jobs, prevent duplicates". */
describe('scheduling a refresh batch', () => {
  beforeEach(async () => {
    await resetDb();
  });


  it('creates one job per due patient', async () => {
    const result = await scheduleRefreshBatch();
    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM refresh_jobs`);

    expect(result.scheduled.length).toBeGreaterThan(0);
    expect(rows[0]?.n).toBe(result.scheduled.length);
  });

  it('records the due studies coalesced into the job', async () => {
    const pid = await patientId('pat-0015');
    await scheduleRefreshBatch({ patientIds: [pid] });

    const job = await jobForPatient(pid);
    expect(job?.coalesced_study_ids.length).toBe(3);
  });

  it('never creates a second job for a patient who already has one', async () => {
    const pid = await patientId('pat-0001');

    const first = await scheduleRefreshBatch({ patientIds: [pid] });
    const second = await scheduleRefreshBatch({ patientIds: [pid] });

    expect(first.scheduled).toHaveLength(1);
    expect(second.scheduled).toHaveLength(0);

    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM refresh_jobs WHERE patient_id=$1`, [pid]);
    expect(rows[0]?.n).toBe(1);
  });

  /**
   * The race the partial unique index exists for. Two schedulers can both
   * observe "no active job" before either inserts; only the index can settle it.
   */
  it('is safe to run several schedulers at the same instant', async () => {
    const results = await Promise.all([
      scheduleRefreshBatch(),
      scheduleRefreshBatch(),
      scheduleRefreshBatch(),
      scheduleRefreshBatch(),
    ]);

    const totalScheduled = results.reduce((sum, r) => sum + r.scheduled.length, 0);

    const { rows } = await pool.query(
      `SELECT count(*)::int AS total,
              count(DISTINCT patient_id)::int AS distinct_patients
         FROM refresh_jobs WHERE status IN ('pending','in_progress')`,
    );

    // Every row that exists was reported exactly once, and no patient got two.
    expect(rows[0]?.total).toBe(totalScheduled);
    expect(rows[0]?.distinct_patients).toBe(rows[0]?.total);
  });

  it('lets a patient be rescheduled after their job finishes', async () => {
    const pid = await patientId('pat-0001');
    await scheduleRefreshBatch({ patientIds: [pid] });
    // Completed rather than failed: a failure would be quarantined, which is a
    // separate rule tested in eligibility.test.ts. This is about the partial
    // index releasing the patient once the job is terminal.
    await pool.query(`UPDATE refresh_jobs SET status='completed', finished_at=now() WHERE patient_id=$1`, [pid]);
    await pool.query(`UPDATE patient_studies SET last_refresh_at=NULL WHERE patient_id=$1`, [pid]);

    const again = await scheduleRefreshBatch({ patientIds: [pid] });
    expect(again.scheduled).toHaveLength(1);
  });

  /**
   * Regression: load spreading used to schedule jobs into a future their own
   * deadline did not reach.
   *
   * `run_at` was `now() + jitter` (up to an hour for a daily study) while
   * `deadline_at` was a flat `now() + JOB_DEADLINE_SECONDS` (five minutes). With
   * the seeded population that made 44 of 60 jobs expire before any worker was
   * permitted to claim them — the reaper failed them as timeouts having never
   * dispatched a single API call.
   *
   * The whole suite passed anyway, because every other test disables jitter.
   * These use an explicit non-zero window so the two clocks are actually
   * exercised against each other.
   */
  describe('load spreading versus the execution deadline', () => {
    const WINDOW = 3_600;

    it('spreads jobs into the future', async () => {
      const result = await scheduleRefreshBatch({ jitterWindowSeconds: WINDOW });
      const spread = result.scheduled.filter((j) => j.runAt.getTime() > Date.now() + 60_000);
      expect(spread.length).toBeGreaterThan(0);
    });

    it('never creates a job whose deadline precedes its run_at', async () => {
      await scheduleRefreshBatch({ jitterWindowSeconds: WINDOW });

      const { rows } = await pool.query(
        `SELECT count(*)::int AS n FROM refresh_jobs WHERE deadline_at <= run_at`,
      );
      expect(rows[0]?.n).toBe(0);
    });

    it('measures the execution budget from run_at, not from scheduling time', async () => {
      await scheduleRefreshBatch({ jitterWindowSeconds: WINDOW });

      // Every job gets the same budget regardless of how far it was spread.
      const { rows } = await pool.query(
        `SELECT count(*)::int AS n FROM refresh_jobs
          WHERE abs(EXTRACT(EPOCH FROM (deadline_at - run_at)) - $1::float8) > 1`,
        [config.JOB_DEADLINE_SECONDS],
      );
      expect(rows[0]?.n).toBe(0);
    });

    /** The assertion that fails on the old code: it reaped 44 of 60. */
    it('does not reap jittered jobs before they are claimable', async () => {
      const result = await scheduleRefreshBatch({ jitterWindowSeconds: WINDOW });
      expect(result.scheduled.length).toBeGreaterThan(0);

      // Advance past the old flat deadline while most jobs are still waiting
      // for their run_at to arrive.
      await pool.query(
        `UPDATE refresh_jobs
            SET run_at      = run_at      - make_interval(secs => $1::float8 + 1),
                deadline_at = deadline_at - make_interval(secs => $1::float8 + 1)`,
        [config.JOB_DEADLINE_SECONDS],
      );

      const reaped = await failExpiredJobs();
      const stillWaiting = await pool.query(
        `SELECT count(*)::int AS n FROM refresh_jobs WHERE status='pending' AND run_at > now()`,
      );

      // Jobs not yet eligible must still be alive and waiting their turn.
      expect(stillWaiting.rows[0]?.n).toBeGreaterThan(0);
      expect(reaped).toBeLessThan(result.scheduled.length);
    });
  });

  describe('user-triggered refresh', () => {
    it('schedules a patient who is not due, at the requested priority', async () => {
      const pid = await patientId('pat-0001');
      const daily = await studyId('cardiology-longitudinal');
      await pool.query(`UPDATE patient_studies SET last_refresh_at = now() WHERE patient_id=$1 AND study_id=$2`, [
        pid,
        daily,
      ]);

      // Not due by frequency, but a human asked for it.
      expect((await scheduleRefreshBatch({ patientIds: [pid] })).scheduled).toHaveLength(0);

      const forced = await scheduleRefreshBatch({ patientIds: [pid], priorityOverride: 5000 });
      expect(forced.scheduled).toHaveLength(1);
      expect(forced.scheduled[0]?.priority).toBe(5000);
    });

    it('carries the patient\'s active enrollments onto a forced job', async () => {
      const pid = await patientId('pat-0015');
      const forced = await scheduleRefreshBatch({ patientIds: [pid], priorityOverride: 5000 });
      expect(forced.scheduled[0]?.studyIds.length).toBe(3);
    });

    it('does not defer a user-triggered refresh behind load spreading', async () => {
      const pid = await patientId('pat-0001');
      const before = Date.now();
      const forced = await scheduleRefreshBatch({ patientIds: [pid], priorityOverride: 5000 });

      // run_at is "now", not now + jitter.
      expect(forced.scheduled[0]!.runAt.getTime()).toBeLessThanOrEqual(before + 2_000);
    });

    /**
     * The quarantine stops the *scheduler* from looping on a doomed patient. It
     * must not stop a human who has just fixed the underlying problem —
     * re-granted consent, corrected a record — from asking for a retry.
     */
    it('overrides the quarantine that follows a permanent failure', async () => {
      const pid = await patientId('pat-0001');
      await pool.query(
        `INSERT INTO refresh_jobs (patient_id, coalesced_study_ids, status, priority, run_at,
                                   deadline_at, max_attempts, failure_class, finished_at)
         VALUES ($1, '{}', 'failed', 100, now(), now() + interval '300 s', 3, 'permanent', now())`,
        [pid],
      );

      // The scheduler will not touch them...
      expect((await scheduleRefreshBatch({ patientIds: [pid] })).scheduled).toHaveLength(0);
      // ...but an explicit request still goes through.
      expect(
        (await scheduleRefreshBatch({ patientIds: [pid], priorityOverride: 5000 })).scheduled,
      ).toHaveLength(1);
    });

    it('refuses to duplicate a refresh that is already in flight', async () => {
      const pid = await patientId('pat-0001');
      await scheduleRefreshBatch({ patientIds: [pid] });

      const forced = await scheduleRefreshBatch({ patientIds: [pid], priorityOverride: 5000 });
      expect(forced.scheduled).toHaveLength(0);
    });
  });
});

/**
 * Jitter is unit-tested directly because the integration tests deliberately run
 * with it disabled — a jittered run_at would make every claim test racy.
 */
describe('load spreading', () => {
  it('places a patient at the same offset every cycle', () => {
    const a = jitterSeconds(42, 86_400, 0.1, 3_600);
    const b = jitterSeconds(42, 86_400, 0.1, 3_600);
    expect(a).toBe(b);
  });

  it('places different patients at different offsets', () => {
    const offsets = new Set(Array.from({ length: 200 }, (_, i) => Math.floor(jitterSeconds(i, 86_400, 0.1, 3_600))));
    // Spread, not clustered: hundreds of patients should not share a handful of slots.
    expect(offsets.size).toBeGreaterThan(150);
  });

  it('keeps the window inside the configured cap', () => {
    for (let i = 0; i < 500; i++) {
      expect(jitterSeconds(i, 86_400, 0.1, 3_600)).toBeLessThanOrEqual(3_600);
    }
  });

  it('scales the window to the tightest interval, not a fixed value', () => {
    // A two-minute study must not be spread over an hour.
    for (let i = 0; i < 200; i++) {
      expect(jitterSeconds(i, 120, 0.1, 3_600)).toBeLessThanOrEqual(12);
    }
  });
});
