import { withTransaction, pool } from '../db.js';
import { config } from '../config.js';
import type { ClaimedJob } from '../types.js';

/**
 * CORE OPERATION 3 — claim jobs for processing.
 *
 * "Workers claim jobs atomically (no double-processing), respecting priority
 * order."
 *
 * Two mechanisms do the work here, and they are separable:
 *
 * 1. ATOMIC CLAIM. `FOR UPDATE SKIP LOCKED` lets N workers pull disjoint sets
 *    of rows from one queue in one statement. A row locked by another worker's
 *    in-flight transaction is skipped rather than waited on, so workers never
 *    queue behind each other and never see the same job. This is why the
 *    project uses Postgres at all — it is the whole feature.
 *
 * 2. RATE LIMIT ADMISSION. A patient-level refresh fans out server-side to
 *    every endpoint holding that patient's data, so dispatching one costs one
 *    token from EACH of those buckets, all or nothing. Doing this inside the
 *    claim transaction means a job can never be in-flight without its budget
 *    having been paid for — there is no window where we have claimed work we
 *    are not allowed to send. HealthEx confirmed that individual EHR rate
 *    limits must still be handled even though the call is patient-level.
 *
 *    NOTE — an assumption, deliberately conservative. Every claim goes through
 *    this admission check, including the claims that only perform a STATUS
 *    POLL. So a poll costs a token from each endpoint too. Whether real vendors
 *    charge polls against the same budget was the one part of the rate-limit
 *    question HealthEx did not answer. If polls are free, this throttles us
 *    harder than necessary and the fix is one condition here; if they are not
 *    free and we assumed they were, we would breach a vendor limit. Erring
 *    toward the recoverable failure.
 *
 * Lock ordering, which is the thing that would otherwise deadlock: job rows are
 * locked first (via SKIP LOCKED, so no worker ever blocks), then buckets in
 * ascending endpoint_id order. Every transaction in the system acquires bucket
 * locks in that same order, so no cycle can form.
 */

export interface ClaimResult {
  claimed: ClaimedJob[];
  /** Jobs we could have run but had no rate-limit budget for; returned to the queue. */
  deferredForRateLimit: number;
}

export async function claimJobs(workerId: string, batchSize: number = config.WORKER_BATCH_SIZE): Promise<ClaimResult> {
  return withTransaction(async (tx) => {
    // -- 1. Provisional claim ------------------------------------------------
    // Note what is claimable: pending work whose run_at has arrived, AND
    // in_progress work whose lease has lapsed. The second case is how a crashed
    // worker's jobs come back — recovery needs no separate reaper process,
    // because the ordinary claim path already looks for it.
    const { rows: provisional } = await tx.query(
      `WITH candidate AS (
         SELECT id
           FROM refresh_jobs
          WHERE status IN ('pending', 'in_progress')
            AND run_at <= now()
            AND deadline_at > now()
            AND (lease_expires_at IS NULL OR lease_expires_at <= now())
          ORDER BY priority DESC, run_at ASC, id ASC
          FOR UPDATE SKIP LOCKED
          LIMIT $2
       ),
       claimed AS (
         UPDATE refresh_jobs j
            SET status           = 'in_progress',
                locked_by        = $1,
                lease_expires_at = now() + make_interval(secs => $3::float8),
                updated_at       = now()
           FROM candidate c
          WHERE j.id = c.id
         RETURNING j.*
       )
       SELECT c.*, p.external_ref
         FROM claimed c
         JOIN patients p ON p.id = c.patient_id
        ORDER BY c.priority DESC, c.run_at ASC, c.id ASC`,
      [workerId, batchSize, config.LEASE_SECONDS],
    );

    if (provisional.length === 0) return { claimed: [], deferredForRateLimit: 0 };

    // -- 2. Which buckets does each of these calls spend? ---------------------
    const patientIds = provisional.map((r) => r.patient_id);
    const { rows: assoc } = await tx.query(
      `SELECT patient_id, array_agg(endpoint_id ORDER BY endpoint_id) AS endpoint_ids
         FROM patient_endpoints
        WHERE patient_id = ANY($1::bigint[])
        GROUP BY patient_id`,
      [patientIds],
    );
    const endpointsByPatient = new Map<number, number[]>(assoc.map((r) => [r.patient_id, r.endpoint_ids]));

    // -- 3. Lock and lazily refill every bucket we might touch ---------------
    // Ascending endpoint_id, one statement each. Refilling on access rather
    // than on a timer means there is no background job to fall behind, and a
    // bucket that nobody touches costs nothing.
    const needed = [...new Set(assoc.flatMap((r) => r.endpoint_ids as number[]))].sort((a, b) => a - b);
    const tokens = new Map<number, number>();
    for (const endpointId of needed) {
      // Self-heal a missing bucket. A newly inserted ehr_endpoints row with no
      // matching bucket read as "zero tokens", which is indistinguishable from
      // "throttled" — so every patient on that endpoint deferred forever with
      // no error anywhere. Deriving the bucket from the endpoint's own declared
      // limit makes the two states impossible to confuse.
      await tx.query(
        `INSERT INTO rate_limit_buckets (endpoint_id, tokens, capacity, refill_per_sec)
         SELECT id, rate_limit_per_min, rate_limit_per_min, rate_limit_per_min / 60.0
           FROM ehr_endpoints WHERE id = $1
         ON CONFLICT (endpoint_id) DO NOTHING`,
        [endpointId],
      );

      const { rows } = await tx.query(
        `UPDATE rate_limit_buckets
            SET tokens = LEAST(capacity,
                               tokens + EXTRACT(EPOCH FROM (now() - last_refill_at)) * refill_per_sec),
                last_refill_at = now()
          WHERE endpoint_id = $1
        RETURNING tokens`,
        [endpointId],
      );
      tokens.set(endpointId, rows[0]?.tokens ?? 0);
    }

    // -- 4. Spend budget, highest priority first -----------------------------
    // Greedy in priority order, so when budget is scarce it goes to the most
    // urgent patients rather than to whoever happens to sort first by id.
    const keep: ClaimedJob[] = [];
    const defer: number[] = [];
    const noEndpoint: number[] = [];

    for (const row of provisional) {
      const endpoints = endpointsByPatient.get(row.patient_id) ?? [];

      // A patient with no endpoint association has nowhere to fetch from. This
      // used to fall through to `[].every(...) === true`, which admitted the job
      // free of charge — and because the source only sees a patient reference,
      // it could answer "success" and we would advance the enrollment clocks
      // for data nobody ever retrieved. Marking a data-quality problem as fresh
      // clinical data is the worst outcome available here, so fail it loudly
      // and never make the call.
      if (endpoints.length === 0) {
        noEndpoint.push(row.id);
        continue;
      }

      const affordable = endpoints.every((id) => (tokens.get(id) ?? 0) >= 1);

      if (affordable) {
        for (const id of endpoints) tokens.set(id, (tokens.get(id) ?? 0) - 1);
        keep.push(row as ClaimedJob);
      } else {
        defer.push(row.id);
      }
    }

    // -- 5. Persist token spend ----------------------------------------------
    for (const endpointId of needed) {
      await tx.query(`UPDATE rate_limit_buckets SET tokens = $2 WHERE endpoint_id = $1`, [
        endpointId,
        tokens.get(endpointId),
      ]);
    }

    // -- 6. Hand back what we cannot afford ----------------------------------
    // These were never visible to another worker: they were locked by this
    // transaction the whole time, so on commit they simply reappear as pending.
    if (defer.length > 0) {
      await tx.query(
        `UPDATE refresh_jobs
            SET status           = 'pending',
                locked_by        = NULL,
                lease_expires_at = NULL,
                run_at           = now() + make_interval(secs => $2::float8),
                -- The deadline is a budget for EXECUTING, not for waiting for
                -- permission to execute. Without this, a job starved of
                -- rate-limit budget burns its deadline sitting in the queue and
                -- is eventually failed as a 'timeout' having never been allowed
                -- to send a single request.
                deadline_at      = deadline_at + make_interval(secs => $2::float8),
                failure_class    = 'rate_limited',
                last_error       = 'deferred: no rate-limit budget for one of this patient''s endpoints',
                updated_at       = now()
          WHERE id = ANY($1::bigint[])`,
        [defer, config.RATE_LIMIT_COOLDOWN_SECONDS],
      );
    }

    if (noEndpoint.length > 0) {
      await tx.query(
        `UPDATE refresh_jobs
            SET status           = 'failed',
                locked_by        = NULL,
                lease_expires_at = NULL,
                failure_class    = 'permanent',
                last_error       = 'no EHR endpoint is associated with this patient',
                finished_at      = now(),
                updated_at       = now()
          WHERE id = ANY($1::bigint[])`,
        [noEndpoint],
      );
    }

    return { claimed: keep, deferredForRateLimit: defer.length };
  });
}

/**
 * Give up every lease this worker holds, immediately.
 *
 * Called on SIGTERM. Without it a rolling deploy parks each in-flight job for a
 * full lease duration; with it the work is claimable by a surviving worker on
 * the next poll. Purely an availability optimisation — correctness is already
 * covered by lease expiry.
 */
export async function releaseLeases(workerId: string): Promise<number> {
  const { rowCount } = await pool.query(
    `UPDATE refresh_jobs
        SET lease_expires_at = now(), locked_by = NULL, updated_at = now()
      WHERE locked_by = $1 AND status = 'in_progress'`,
    [workerId],
  );
  return rowCount ?? 0;
}
