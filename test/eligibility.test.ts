import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { checkEligibility, findDuePatients } from '../src/domain/eligibility.js';
import { insertJob, patientId, resetDb, setLastRefresh, studyId } from './helpers.js';
import { pool } from '../src/db.js';

/** CORE OPERATION 1 — "determine if refresh is needed based on configured frequency". */
describe('refresh eligibility', () => {
  let daily: number;
  let weekly: number;
  let acute: number;

  beforeAll(async () => {
    await resetDb();
    daily = await studyId('cardiology-longitudinal'); // 1 day
    weekly = await studyId('oncology-weekly-panel'); // 7 days
    acute = await studyId('acute-monitoring'); // 2 minutes
  });

  beforeEach(async () => {
    await resetDb();
  });


  it('is due when the patient has never been refreshed', async () => {
    const pid = await patientId('pat-0001');
    const result = await checkEligibility(pid, daily);

    expect(result?.isDue).toBe(true);
    expect(result?.lastRefreshAt).toBeNull();
    expect(result?.reason).toBe('due for refresh');
  });

  it('is not due inside the configured interval', async () => {
    const pid = await patientId('pat-0001');
    await setLastRefresh(pid, daily, '1 hour');

    const result = await checkEligibility(pid, daily);
    expect(result?.isDue).toBe(false);
    expect(result?.reason).toBe('refreshed within the configured interval');
  });

  it('is due once the interval has elapsed', async () => {
    const pid = await patientId('pat-0001');
    await setLastRefresh(pid, daily, '25 hours');

    const result = await checkEligibility(pid, daily);
    expect(result?.isDue).toBe(true);
  });

  it('honours each study\'s own frequency for the same patient', async () => {
    const pid = await patientId('pat-0005'); // enrolled in daily + acute
    await setLastRefresh(pid, daily, '1 hour');
    await setLastRefresh(pid, acute, '1 hour');

    // One hour is nothing to a daily study and an age to a two-minute one.
    expect((await checkEligibility(pid, daily))?.isDue).toBe(false);
    expect((await checkEligibility(pid, acute))?.isDue).toBe(true);
  });

  it('is never due for a withdrawn enrollment', async () => {
    const pid = await patientId('pat-0001');
    await pool.query(`UPDATE patient_studies SET status='withdrawn' WHERE patient_id=$1 AND study_id=$2`, [pid, daily]);

    const result = await checkEligibility(pid, daily);
    expect(result?.isDue).toBe(false);
    expect(result?.reason).toBe('enrollment is not active');
  });

  it('reports an in-flight refresh separately from being due', async () => {
    const pid = await patientId('pat-0001');
    await insertJob({ patientId: pid });

    const result = await checkEligibility(pid, daily);
    expect(result?.isDue).toBe(true); // still stale...
    expect(result?.hasActiveJob).toBe(true); // ...but already being handled
    expect(result?.reason).toBe('due, but a refresh is already scheduled or running');
  });

  it('returns null for an enrollment that does not exist', async () => {
    const pid = await patientId('pat-0001');
    expect(await checkEligibility(pid, weekly)).toBeNull(); // pat-0001 is not in the weekly study
  });

  describe('bulk selection for the scheduler', () => {
    it('coalesces every due study for a patient into one refresh', async () => {
      const pid = await patientId('pat-0015'); // daily + weekly + acute
      const due = await findDuePatients({ patientIds: [pid] });

      expect(due).toHaveLength(1);
      // One entry, three studies — one API call will satisfy all of them.
      expect(due[0]?.studyIds.sort()).toEqual([daily, weekly, acute].sort());
    });

    it('excludes studies that are not themselves due', async () => {
      const pid = await patientId('pat-0015');
      await setLastRefresh(pid, daily, '1 hour');
      await setLastRefresh(pid, weekly, '1 hour');
      await setLastRefresh(pid, acute, '1 hour'); // only this one is stale

      const due = await findDuePatients({ patientIds: [pid] });
      expect(due[0]?.studyIds).toEqual([acute]);
    });

    it('takes the highest priority among the due studies', async () => {
      const pid = await patientId('pat-0015');
      const due = await findDuePatients({ patientIds: [pid] });

      // acute-monitoring has base_priority 200, the others 100 and 120.
      expect(due[0]?.priority).toBe(200);
    });

    it('boosts priority when consent is about to expire', async () => {
      // pat-0011 is seeded with consent expiring in 3 days.
      const urgent = await patientId('pat-0011');
      const normal = await patientId('pat-0001');

      const due = await findDuePatients({ patientIds: [urgent, normal] });
      const urgentRow = due.find((d) => d.patientId === urgent);
      const normalRow = due.find((d) => d.patientId === normal);

      expect(urgentRow?.priority).toBe(600); // 100 base + 500 boost
      expect(normalRow?.priority).toBe(100);
      // ...and urgency actually changes the order the scheduler emits.
      expect(due[0]?.patientId).toBe(urgent);
    });

    it('skips patients that already have an active job', async () => {
      const pid = await patientId('pat-0001');
      await insertJob({ patientId: pid });

      const due = await findDuePatients({ patientIds: [pid] });
      expect(due).toHaveLength(0);
    });

    /**
     * Regression: found by running the system, not by a unit test.
     *
     * A terminal failure never advances last_refresh_at, so the patient stays
     * "due" forever. Before the quarantine existed the scheduler recreated the
     * same doomed job on every tick — one observed patient accumulated four
     * permanent-failure jobs in 14 seconds, each one a paid API call. The
     * job-level rule ("permanent failures are not retried") was correct; the
     * loop above it was not.
     */
    describe('suppression after a terminal failure', () => {
      it('does not immediately reschedule a permanently failed patient', async () => {
        const pid = await patientId('pat-0001');
        const job = await insertJob({ patientId: pid });
        await pool.query(
          `UPDATE refresh_jobs SET status='failed', failure_class='permanent', finished_at=now() WHERE id=$1`,
          [job.id],
        );

        expect(await findDuePatients({ patientIds: [pid] })).toHaveLength(0);
      });

      it('does not immediately reschedule after the retry budget is exhausted', async () => {
        const pid = await patientId('pat-0001');
        const job = await insertJob({ patientId: pid });
        await pool.query(
          `UPDATE refresh_jobs SET status='failed', failure_class='transient', finished_at=now() WHERE id=$1`,
          [job.id],
        );

        expect(await findDuePatients({ patientIds: [pid] })).toHaveLength(0);
      });

      it('reconsiders the patient once the quarantine window has passed', async () => {
        const pid = await patientId('pat-0001');
        const job = await insertJob({ patientId: pid });
        // Transient window is 1s under test config; put the failure well behind it.
        await pool.query(
          `UPDATE refresh_jobs
              SET status='failed', failure_class='transient', finished_at = now() - interval '10 seconds'
            WHERE id=$1`,
          [job.id],
        );

        expect(await findDuePatients({ patientIds: [pid] })).toHaveLength(1);
      });

      it('quarantines a permanent failure for longer than a transient one', async () => {
        const pid = await patientId('pat-0001');
        const job = await insertJob({ patientId: pid });
        // Past the transient window (1s) but inside the permanent window (60s).
        await pool.query(
          `UPDATE refresh_jobs
              SET status='failed', failure_class='permanent', finished_at = now() - interval '10 seconds'
            WHERE id=$1`,
          [job.id],
        );

        expect(await findDuePatients({ patientIds: [pid] })).toHaveLength(0);
      });

      it('only considers the most recent outcome', async () => {
        const pid = await patientId('pat-0001');
        const old = await insertJob({ patientId: pid });
        await pool.query(
          `UPDATE refresh_jobs SET status='failed', failure_class='permanent',
                  finished_at = now() - interval '5 seconds' WHERE id=$1`,
          [old.id],
        );
        const recent = await insertJob({ patientId: pid });
        await pool.query(
          `UPDATE refresh_jobs SET status='completed', finished_at = now() - interval '1 second' WHERE id=$1`,
          [recent.id],
        );
        await pool.query(`UPDATE patient_studies SET last_refresh_at=NULL WHERE patient_id=$1`, [pid]);

        // An old failure followed by a success must not keep the patient out.
        expect(await findDuePatients({ patientIds: [pid] })).toHaveLength(1);
      });

      it('explains itself through the eligibility check', async () => {
        const pid = await patientId('pat-0001');
        const job = await insertJob({ patientId: pid });
        await pool.query(
          `UPDATE refresh_jobs SET status='failed', failure_class='permanent', finished_at=now() WHERE id=$1`,
          [job.id],
        );

        const result = await checkEligibility(pid, daily);
        expect(result?.isDue).toBe(true); // the data really is stale...
        expect(result?.quarantinedUntil).not.toBeNull(); // ...we are just not acting on it
        expect(result?.reason).toBe('due, but suppressed after a recent terminal failure');
      });
    });

    it('reconsiders a patient once their job reaches a terminal state', async () => {
      const pid = await patientId('pat-0001');
      const job = await insertJob({ patientId: pid });
      await pool.query(`UPDATE refresh_jobs SET status='completed', finished_at=now() WHERE id=$1`, [job.id]);

      const due = await findDuePatients({ patientIds: [pid] });
      expect(due).toHaveLength(1);
    });
  });
});
