import { pool, type Queryable } from '../db.js';
import { config } from '../config.js';

/**
 * CORE OPERATION 1 — determine refresh eligibility.
 *
 * "Given a patient and study, determine if refresh is needed based on
 * configured frequency."
 *
 * Eligibility is a property of an *enrollment*: the study supplies the
 * frequency, the enrollment supplies when it was last satisfied. A patient is
 * worth refreshing when at least one of their active enrollments has gone
 * stale — and when that happens, every enrollment that is also stale rides
 * along on the same API call.
 *
 * Note the asymmetry with completion. Eligibility only ever looks at DUE
 * enrollments, because only a due enrollment justifies paying for a call. But
 * once the call succeeds it has refreshed the patient's data outright, so
 * completion advances every ACTIVE association, due or not — see
 * src/domain/complete.ts. The due set is what triggers a refresh; the active
 * set is what a refresh satisfies.
 */

export interface Eligibility {
  patientId: number;
  studyId: number;
  enrolled: boolean;
  lastRefreshAt: Date | null;
  refreshIntervalSeconds: number;
  dueAt: Date | null;
  isDue: boolean;
  hasActiveJob: boolean;
  /** Set when a recent terminal failure is suppressing rescheduling. */
  quarantinedUntil: Date | null;
  reason: string;
}

export async function checkEligibility(patientId: number, studyId: number, db: Queryable = pool): Promise<Eligibility | null> {
  const { rows } = await db.query(
    `SELECT ps.patient_id,
            ps.study_id,
            ps.status = 'active'                            AS enrolled,
            ps.last_refresh_at,
            EXTRACT(EPOCH FROM s.refresh_interval)::float8  AS refresh_interval_seconds,
            ps.last_refresh_at + s.refresh_interval         AS due_at,
            (ps.last_refresh_at IS NULL
              OR ps.last_refresh_at + s.refresh_interval <= now())
                                                            AS interval_elapsed,
            EXISTS (
              SELECT 1 FROM refresh_jobs j
              WHERE j.patient_id = ps.patient_id
                AND j.status IN ('pending', 'in_progress')
            )                                               AS has_active_job,
            (SELECT CASE WHEN j.status = 'failed'
                         THEN j.finished_at + make_interval(secs =>
                                CASE WHEN j.failure_class = 'permanent'
                                     THEN $3::float8 ELSE $4::float8 END)
                    END
               FROM refresh_jobs j
              WHERE j.patient_id = ps.patient_id AND j.finished_at IS NOT NULL
              ORDER BY j.finished_at DESC
              LIMIT 1)                                      AS quarantined_until
       FROM patient_studies ps
       JOIN studies s ON s.id = ps.study_id
      WHERE ps.patient_id = $1 AND ps.study_id = $2`,
    [patientId, studyId, config.PERMANENT_QUARANTINE_SECONDS, config.TRANSIENT_QUARANTINE_SECONDS],
  );

  const row = rows[0];
  if (!row) return null;

  const isDue: boolean = row.enrolled && row.interval_elapsed;
  const quarantinedUntil: Date | null = row.quarantined_until;
  const quarantined = quarantinedUntil !== null && quarantinedUntil.getTime() > Date.now();

  return {
    patientId: row.patient_id,
    studyId: row.study_id,
    enrolled: row.enrolled,
    lastRefreshAt: row.last_refresh_at,
    refreshIntervalSeconds: row.refresh_interval_seconds,
    dueAt: row.due_at,
    isDue,
    hasActiveJob: row.has_active_job,
    quarantinedUntil: quarantined ? quarantinedUntil : null,
    reason: !row.enrolled
      ? 'enrollment is not active'
      : !row.interval_elapsed
        ? 'refreshed within the configured interval'
        : row.has_active_job
          ? 'due, but a refresh is already scheduled or running'
          : quarantined
            ? 'due, but suppressed after a recent terminal failure'
            : 'due for refresh',
  };
}

/** One patient who needs refreshing, with all their due enrollments folded in. */
export interface DuePatient {
  patientId: number;
  externalRef: string;
  studyIds: number[];
  priority: number;
  tightestIntervalSeconds: number;
}

/**
 * The bulk form of the same question, used by the scheduler.
 *
 * Two things happen in the aggregate that do not happen per-enrollment:
 *   - due studies for a patient collapse into one row, so the patient gets one
 *     API call rather than one per study (the "avoid redundant work" rule);
 *   - priority becomes the MAX across those studies, so a patient's refresh
 *     inherits the urgency of the most urgent thing waiting on it.
 *
 * Patients with a live job are filtered out here. That is an optimisation, not
 * the correctness mechanism — the unique index is what actually prevents
 * duplicates when two schedulers run this at the same instant.
 */
export async function findDuePatients(
  opts: { patientIds?: number[]; limit?: number } = {},
  db: Queryable = pool,
): Promise<DuePatient[]> {
  const { rows } = await db.query(
    `WITH due AS (
       SELECT ps.patient_id,
              ps.study_id,
              s.base_priority
                + CASE
                    WHEN ps.consent_expires_at IS NOT NULL
                     AND ps.consent_expires_at <= now() + make_interval(days => $3::int)
                    THEN $4::int ELSE 0
                  END                                        AS priority,
              EXTRACT(EPOCH FROM s.refresh_interval)::float8 AS interval_seconds
         FROM patient_studies ps
         JOIN studies s ON s.id = ps.study_id
        WHERE ps.status = 'active'
          AND (ps.last_refresh_at IS NULL
               OR ps.last_refresh_at + s.refresh_interval <= now())
          AND ($1::bigint[] IS NULL OR ps.patient_id = ANY($1::bigint[]))
     ),
     -- Most recent finished job per candidate patient. A terminal failure does
     -- not advance last_refresh_at, so without this the patient stays "due"
     -- forever and the scheduler recreates the same doomed job every tick —
     -- turning "permanent failures should not retry" into an infinite retry
     -- loop one level up, and paying for the API call every time.
     latest_terminal AS (
       SELECT DISTINCT ON (j.patient_id)
              j.patient_id, j.status, j.failure_class, j.finished_at
         FROM refresh_jobs j
        WHERE j.finished_at IS NOT NULL
          AND j.patient_id IN (SELECT patient_id FROM due)
        ORDER BY j.patient_id, j.finished_at DESC
     )
     SELECT d.patient_id,
            p.external_ref,
            array_agg(d.study_id ORDER BY d.study_id) AS study_ids,
            MAX(d.priority)::int                      AS priority,
            MIN(d.interval_seconds)::float8           AS tightest_interval_seconds
       FROM due d
       JOIN patients p ON p.id = d.patient_id
       LEFT JOIN latest_terminal lt ON lt.patient_id = d.patient_id
      WHERE NOT EXISTS (
              SELECT 1 FROM refresh_jobs j
               WHERE j.patient_id = d.patient_id
                 AND j.status IN ('pending', 'in_progress')
            )
        AND (
              lt.patient_id IS NULL
           OR lt.status = 'completed'
           OR lt.finished_at <= now() - make_interval(secs =>
                CASE WHEN lt.failure_class = 'permanent' THEN $5::float8 ELSE $6::float8 END)
            )
      GROUP BY d.patient_id, p.external_ref
      ORDER BY MAX(d.priority) DESC, d.patient_id
      LIMIT $2`,
    [
      opts.patientIds ?? null,
      opts.limit ?? config.SCHEDULER_BATCH_LIMIT,
      config.CONSENT_URGENCY_DAYS,
      config.CONSENT_PRIORITY_BOOST,
      config.PERMANENT_QUARANTINE_SECONDS,
      config.TRANSIENT_QUARANTINE_SECONDS,
    ],
  );

  return rows.map((r) => ({
    patientId: r.patient_id,
    externalRef: r.external_ref,
    studyIds: r.study_ids,
    priority: r.priority,
    tightestIntervalSeconds: r.tightest_interval_seconds,
  }));
}
