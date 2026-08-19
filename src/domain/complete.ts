import { withTransaction, pool, type Queryable } from '../db.js';
import { config } from '../config.js';
import { unitHash } from './jitter.js';
import type { ClaimedJob, RefreshOutcome } from '../types.js';

/**
 * CORE OPERATION 4 — complete a job.
 *
 * "Record success or failure, schedule retry with backoff if transient."
 *
 * This is the only place job state moves after a claim, which is deliberate:
 * one function holds the entire state machine, so the retry policy can be read
 * top to bottom and tested without a network or a worker.
 *
 * The classification that matters:
 *
 *   transient    -> retry with exponential backoff until max_attempts.
 *   permanent    -> fail now. Retrying a revoked consent or an unknown patient
 *                   just spends money to receive the same 4xx.
 *   rate_limited -> retry, but do NOT spend a retry attempt. Being throttled is
 *                   a statement about our request rate, not about this job. A
 *                   job that burned its budget on 429s would be killed by our
 *                   own traffic shaping, which is backwards.
 *   accepted /
 *   still_running-> not terminal. Release the lease, set a poll time, and let
 *                   the job go back in the queue for anyone to pick up.
 */

/**
 * Exponential backoff with deterministic jitter.
 *
 * Jitter matters because failures correlate: when an EHR has an outage, every
 * job fails within the same second and would otherwise retry in lockstep
 * forever. Deriving it from the job id instead of Math.random() keeps that
 * spreading property while leaving tests reproducible.
 */
export function computeBackoffSeconds(
  attempts: number,
  jobId: number,
  baseSeconds: number = config.RETRY_BASE_SECONDS,
  maxSeconds: number = config.RETRY_MAX_SECONDS,
): number {
  const raw = Math.min(maxSeconds, baseSeconds * 2 ** Math.max(0, attempts - 1));
  const jitterFactor = 0.75 + 0.5 * unitHash(jobId, attempts); // ±25%, stable per (job, attempt)
  return Math.min(maxSeconds, raw * jitterFactor);
}

/**
 * Records the result of an attempt. Returns false when the write was refused
 * because this worker no longer holds the lease.
 *
 * FENCING. Every statement below is guarded by `locked_by = <the worker that
 * claimed it>`. Without that guard a worker which stalled long enough for its
 * lease to lapse could wake up and overwrite the work of the worker that
 * legitimately reclaimed the job — resetting status, clearing a live
 * external_request_id and double-counting attempts. The lease makes a job
 * *reclaimable*; only this guard makes the reclaim *safe*.
 */
export async function recordOutcome(job: ClaimedJob, outcome: RefreshOutcome, db?: Queryable): Promise<boolean> {
  const run = db
    ? (fn: (tx: Queryable) => Promise<boolean>) => fn(db)
    : (fn: (tx: Queryable) => Promise<boolean>) => withTransaction(fn);

  return run(async (tx) => {
    switch (outcome.kind) {
      // -- Not terminal: the EHR took the request and is working on it --------
      case 'accepted': {
        const r = await tx.query(
          `UPDATE refresh_jobs
              SET external_request_id = $2,
                  attempts            = attempts + 1,
                  locked_by           = NULL,
                  lease_expires_at    = NULL,
                  run_at              = now() + make_interval(secs => $3::float8),
                  failure_class       = NULL,
                  last_error          = NULL,
                  updated_at          = now()
            WHERE id = $1 AND locked_by = $4`,
          [job.id, outcome.requestId, config.POLL_INTERVAL_SECONDS, job.locked_by],
        );
        return (r.rowCount ?? 0) > 0;
      }

      // -- Not terminal: we never learned whether the send landed -------------
      // Do NOT advance `attempts`. The idempotency key is (job, attempts), so
      // advancing it would send a *different* key and the source would treat
      // the retry as new paid work. Retrying the identical key is the whole
      // point. Bounded by deadline_at rather than by the retry budget, because
      // this is not a failure of the refresh.
      case 'ambiguous': {
        // Exponential in its OWN counter, not in `attempts` — advancing
        // `attempts` would rotate the idempotency key, which is the one thing
        // this outcome exists to prevent. A fixed delay here would hammer a
        // source that is merely slow.
        const backoff = computeBackoffSeconds(job.ambiguous_attempts + 1, job.id);
        const r = await tx.query(
          `UPDATE refresh_jobs
              SET status             = 'pending',
                  locked_by          = NULL,
                  lease_expires_at   = NULL,
                  ambiguous_attempts = ambiguous_attempts + 1,
                  run_at             = now() + make_interval(secs => $3::float8),
                  last_error         = $2,
                  updated_at         = now()
            WHERE id = $1 AND locked_by = $4`,
          [job.id, outcome.message, backoff, job.locked_by],
        );
        return (r.rowCount ?? 0) > 0;
      }

      // -- Not terminal: still retrieving, or we failed to observe it ---------
      case 'still_running':
      case 'poll_deferred': {
        const r = await tx.query(
          `UPDATE refresh_jobs
              SET locked_by        = NULL,
                  lease_expires_at = NULL,
                  run_at           = now() + make_interval(secs => $2::float8),
                  last_error       = $3,
                  updated_at       = now()
            WHERE id = $1 AND locked_by = $4`,
          [job.id, config.POLL_INTERVAL_SECONDS, outcome.kind === 'poll_deferred' ? outcome.message : null, job.locked_by],
        );
        return (r.rowCount ?? 0) > 0;
      }

      // -- Terminal success ---------------------------------------------------
      case 'success': {
        const r = await tx.query(
          `UPDATE refresh_jobs
              SET status           = 'completed',
                  locked_by        = NULL,
                  lease_expires_at = NULL,
                  failure_class    = NULL,
                  last_error       = NULL,
                  finished_at      = now(),
                  updated_at       = now()
            WHERE id = $1 AND locked_by = $2`,
          [job.id, job.locked_by],
        );
        if ((r.rowCount ?? 0) === 0) return false; // lease lost: do not touch enrollment clocks

        // Advance every ACTIVE enrollment this patient has — not only the ones
        // that happened to be due when the job was scheduled.
        //
        // The call refreshed the patient's data, so every study association
        // reading that data is now current; leaving a non-due association on an
        // older timestamp would schedule a second, redundant paid call for data
        // we already hold. HealthEx confirmed this reading ("one patient
        // refresh and update all the studies-patient associations").
        //
        // `coalesced_study_ids` is deliberately NOT used here. It stays on the
        // job as an audit record of which due studies caused or were folded
        // into this refresh; it is not the set of associations the refresh
        // satisfies. The two differ whenever a patient has an active study that
        // was still inside its interval.
        //
        // Withdrawn associations are excluded: a withdrawn enrollment has no
        // claim on this data and its timestamp should not imply otherwise.
        //
        // Same transaction as the status change on purpose. If these could
        // diverge, a crash between them would either re-refresh a patient we
        // just paid for, or mark studies fresh for data we never fetched.
        await tx.query(
          `UPDATE patient_studies
              SET last_refresh_at = now()
            WHERE patient_id = $1 AND status = 'active'`,
          [job.patient_id],
        );
        return true;
      }

      // -- Terminal failure, no retry ----------------------------------------
      case 'permanent': {
        const r = await tx.query(
          `UPDATE refresh_jobs
              SET status           = 'failed',
                  locked_by        = NULL,
                  lease_expires_at = NULL,
                  attempts         = attempts + 1,
                  failure_class    = 'permanent',
                  last_error       = $2,
                  finished_at      = now(),
                  updated_at       = now()
            WHERE id = $1 AND locked_by = $3`,
          [job.id, outcome.message, job.locked_by],
        );
        return (r.rowCount ?? 0) > 0;
      }

      // -- Retry, or give up if the budget is spent --------------------------
      case 'transient': {
        const nextAttempt = job.attempts + 1;
        const exhausted = nextAttempt >= job.max_attempts;

        const r = await tx.query(
          `UPDATE refresh_jobs
              SET status              = $3,
                  attempts            = attempts + 1,
                  locked_by           = NULL,
                  lease_expires_at    = NULL,
                  -- Start the next attempt from $updateData, not from a poll.
                  external_request_id = NULL,
                  run_at              = now() + make_interval(secs => $4::float8),
                  failure_class       = 'transient',
                  last_error          = $2,
                  finished_at         = CASE WHEN $3 = 'failed' THEN now() END,
                  updated_at          = now()
            WHERE id = $1 AND locked_by = $5`,
          [
            job.id,
            outcome.message,
            exhausted ? 'failed' : 'pending',
            exhausted ? 0 : computeBackoffSeconds(nextAttempt, job.id),
            job.locked_by,
          ],
        );
        return (r.rowCount ?? 0) > 0;
      }

      // -- Throttled: retry without spending a retry --------------------------
      case 'rate_limited': {
        const r = await tx.query(
          `UPDATE refresh_jobs
              SET status           = 'pending',
                  locked_by        = NULL,
                  lease_expires_at = NULL,
                  run_at           = now() + make_interval(secs => $3::float8),
                  -- Same rule as the claim path: waiting for budget is not
                  -- execution time, so it must not consume the deadline.
                  deadline_at      = deadline_at + make_interval(secs => $3::float8),
                  failure_class    = 'rate_limited',
                  last_error       = $2,
                  updated_at       = now()
            WHERE id = $1 AND locked_by = $4`,
          [job.id, outcome.message, outcome.retryAfterSeconds, job.locked_by],
        );
        return (r.rowCount ?? 0) > 0;
      }
    }
  });
}

/**
 * Fail jobs that have outrun their deadline.
 *
 * Backstop for two things lease expiry alone cannot fix: a retrieval the EHR
 * never finishes (so we would poll forever), and a poison job that kills every
 * worker that claims it (so attempts never increments and it is reclaimed
 * indefinitely). Run from the scheduler tick.
 */
export async function failExpiredJobs(db: Queryable = pool): Promise<number> {
  const { rowCount } = await db.query(
    `UPDATE refresh_jobs
        SET status           = 'failed',
            locked_by        = NULL,
            lease_expires_at = NULL,
            failure_class    = 'timeout',
            last_error       = 'job exceeded its deadline',
            finished_at      = now(),
            updated_at       = now()
      WHERE status IN ('pending', 'in_progress')
        AND (
              deadline_at <= now()
              -- Immutable ceiling. deadline_at is deliberately extended while a
              -- job waits for rate-limit budget, so on its own it is not a
              -- termination guarantee. created_at cannot be moved by anything.
           OR created_at + make_interval(secs => $1::float8) <= now()
            )`,
    [config.MAX_JOB_LIFETIME_SECONDS],
  );
  return rowCount ?? 0;
}
