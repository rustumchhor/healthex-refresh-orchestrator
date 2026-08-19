import { pool, type Queryable } from '../db.js';
import { config } from '../config.js';
import { findDuePatients, type DuePatient } from './eligibility.js';
import { jitterSeconds, unitHash } from './jitter.js';

/**
 * CORE OPERATION 2 — schedule a refresh batch.
 *
 * "Accept a list of patients, create jobs, prevent duplicates."
 *
 * Duplicate prevention is delegated entirely to the partial unique index
 * `refresh_jobs_one_active_per_patient`. Application-level "check then insert"
 * is a race: two schedulers can both read "no active job" before either
 * inserts. `ON CONFLICT DO NOTHING` makes the loser's insert a no-op inside the
 * same statement, so concurrent schedulers need no leader election, no advisory
 * lock and no coordination at all.
 */

export interface ScheduleRequest {
  /** Explicit patient list. Omit to schedule everyone the system finds due. */
  patientIds?: number[];
  /**
   * Forces these patients to be scheduled at this priority even if they are
   * not due yet — this is what "user-triggered" means. Also suppresses jitter,
   * because a human is waiting.
   */
  priorityOverride?: number;
  limit?: number;
  /**
   * Override the load-spreading window, in seconds. Omit to derive it from the
   * tightest due interval. Pass 0 to schedule everything for immediate
   * eligibility — used by the demo, by tests, and by any backfill that must not
   * be spread.
   */
  jitterWindowSeconds?: number;
}

export interface ScheduledJob {
  id: number;
  patientId: number;
  priority: number;
  runAt: Date;
  studyIds: number[];
}

export interface ScheduleResult {
  considered: number;
  scheduled: ScheduledJob[];
  /** Patients that were due but already had a live job. */
  skippedDuplicates: number;
}

export async function scheduleRefreshBatch(req: ScheduleRequest = {}, db: Queryable = pool): Promise<ScheduleResult> {
  const due = req.priorityOverride !== undefined && req.patientIds?.length
    ? await forcePatients(req.patientIds, db)
    : await findDuePatients({ patientIds: req.patientIds, limit: req.limit }, db);

  if (due.length === 0) return { considered: 0, scheduled: [], skippedDuplicates: 0 };

  const payload = due.map((p) => ({
    patientId: p.patientId,
    studyIds: p.studyIds,
    priority: req.priorityOverride ?? p.priority,
    // A user waiting on a manual refresh should not be delayed by load
    // spreading; scheduled work should.
    jitterSeconds:
      req.priorityOverride !== undefined
        ? 0
        : req.jitterWindowSeconds !== undefined
          ? unitHash(p.patientId, 0x5eed) * req.jitterWindowSeconds
          : jitterSeconds(p.patientId, p.tightestIntervalSeconds, config.JITTER_FRACTION, config.JITTER_MAX_SECONDS),
  }));

  const { rows } = await db.query(
    `INSERT INTO refresh_jobs
        (patient_id, coalesced_study_ids, status, priority, run_at, deadline_at, max_attempts)
     SELECT j.patient_id,
            j.study_ids,
            'pending',
            j.priority,
            j.run_at,
            -- The deadline is an EXECUTION budget and must run from the moment
            -- the job becomes eligible, not from the moment it was created.
            -- Measuring it from now() meant a job jittered further into the
            -- future than the deadline was born already expired, and the
            -- deadline reaper failed it as a timeout before any worker was
            -- permitted to claim it.
            j.run_at + make_interval(secs => $2::float8),
            $3::int
       FROM (
         SELECT (r->>'patientId')::bigint                                       AS patient_id,
                ARRAY(SELECT jsonb_array_elements_text(r->'studyIds'))::bigint[] AS study_ids,
                (r->>'priority')::int                                            AS priority,
                now() + make_interval(secs => (r->>'jitterSeconds')::float8)     AS run_at
           FROM jsonb_array_elements($1::jsonb) AS r
       ) j
     ON CONFLICT DO NOTHING
     RETURNING id, patient_id, priority, run_at, coalesced_study_ids`,
    [JSON.stringify(payload), config.JOB_DEADLINE_SECONDS, config.MAX_ATTEMPTS],
  );

  return {
    considered: due.length,
    skippedDuplicates: due.length - rows.length,
    scheduled: rows.map((r) => ({
      id: r.id,
      patientId: r.patient_id,
      priority: r.priority,
      runAt: r.run_at,
      studyIds: r.coalesced_study_ids,
    })),
  };
}

/**
 * Build refresh targets for patients regardless of whether their interval has
 * elapsed. Used only by the user-triggered path, where "is it due?" has already
 * been answered by a human clicking a button.
 *
 * Their active enrollments still come along, so a manual refresh resets the
 * clock on everything it satisfies rather than being wasted work.
 */
async function forcePatients(patientIds: number[], db: Queryable): Promise<DuePatient[]> {
  const { rows } = await db.query(
    `SELECT p.id AS patient_id,
            p.external_ref,
            COALESCE(array_agg(ps.study_id ORDER BY ps.study_id)
                       FILTER (WHERE ps.study_id IS NOT NULL), '{}') AS study_ids,
            COALESCE(MIN(EXTRACT(EPOCH FROM s.refresh_interval))::float8, 0) AS tightest_interval_seconds
       FROM patients p
       LEFT JOIN patient_studies ps
              ON ps.patient_id = p.id AND ps.status = 'active'
       LEFT JOIN studies s ON s.id = ps.study_id
      WHERE p.id = ANY($1::bigint[])
        AND NOT EXISTS (
              SELECT 1 FROM refresh_jobs j
               WHERE j.patient_id = p.id
                 AND j.status IN ('pending', 'in_progress')
            )
      GROUP BY p.id, p.external_ref
      ORDER BY p.id`,
    [patientIds],
  );

  return rows.map((r) => ({
    patientId: r.patient_id,
    externalRef: r.external_ref,
    studyIds: r.study_ids,
    priority: 0, // replaced by priorityOverride
    tightestIntervalSeconds: r.tightest_interval_seconds,
  }));
}
