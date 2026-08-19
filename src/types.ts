export type JobStatus = 'pending' | 'in_progress' | 'completed' | 'failed';

export type FailureClass = 'transient' | 'permanent' | 'rate_limited' | 'timeout';

export interface RefreshJob {
  id: number;
  patient_id: number;
  coalesced_study_ids: number[];
  status: JobStatus;
  priority: number;
  run_at: Date;
  deadline_at: Date;
  attempts: number;
  max_attempts: number;
  ambiguous_attempts: number;
  locked_by: string | null;
  lease_expires_at: Date | null;
  external_request_id: string | null;
  last_error: string | null;
  failure_class: FailureClass | null;
  created_at: Date;
  updated_at: Date;
  finished_at: Date | null;
}

/** A claimed job, plus everything a worker needs to act on it without re-querying. */
export interface ClaimedJob extends RefreshJob {
  external_ref: string;
}

/**
 * The outcomes a refresh attempt can produce. These are the orchestrator's
 * vocabulary, not the EHR's — `ehr/client.ts` is responsible for translating
 * HTTP reality into exactly one of these.
 */
export type RefreshOutcome =
  /** $updateData accepted; the EHR is now retrieving asynchronously. */
  | { kind: 'accepted'; requestId: string }
  /** Poll says the retrieval is still running. Not an error. */
  | { kind: 'still_running' }
  /** Poll says the retrieval finished successfully. */
  | { kind: 'success' }
  /** The retrieval itself failed in a way worth retrying from scratch. */
  | { kind: 'transient'; message: string }
  /** The retrieval failed in a way that will fail identically forever. */
  | { kind: 'permanent'; message: string }
  /** We were throttled. Not the job's fault; does not consume a retry. */
  | { kind: 'rate_limited'; retryAfterSeconds: number; message: string }
  /**
   * The send never got an answer — timeout, reset, refused. We cannot tell
   * whether the source started the work, so this is NOT a failure: retry the
   * SAME logical request with the SAME idempotency key and do not advance the
   * attempt counter.
   */
  | { kind: 'ambiguous'; message: string }
  /**
   * The poll call itself failed at the HTTP level. The retrieval is probably
   * still fine, so we retry the poll rather than restarting the whole refresh.
   */
  | { kind: 'poll_deferred'; message: string };
