import { claimJobs, releaseLeases } from './domain/claim.js';
import { recordOutcome } from './domain/complete.js';
import { EhrClient } from './ehr/client.js';
import { config } from './config.js';
import { logger } from './logger.js';
import type { ClaimedJob } from './types.js';

/**
 * A worker.
 *
 * Workers hold no state worth recovering. Everything durable is a row; the
 * worker's only in-memory possession is a lease, and a lease is designed to be
 * lost. That is what makes horizontal scaling boring: start more processes,
 * stop processes, kill processes — the queue does not care.
 *
 * One unit of work is exactly one HTTP call. That is a deliberate constraint
 * rather than an accident: because no worker ever holds a job for longer than a
 * single request, a fixed lease sized above the HTTP timeout is always
 * sufficient and there is no need for heartbeat renewal. An asynchronous EHR
 * retrieval that takes 30 seconds does not pin a worker for 30 seconds — the
 * job goes back in the queue with a poll time, and whichever worker is free
 * next picks it up.
 */
export class Worker {
  private running = false;
  private loop: Promise<void> | null = null;
  private readonly log;

  constructor(
    readonly id: string,
    private readonly ehr: EhrClient = new EhrClient(config.MOCK_EHR_BASE_URL, config.EHR_REQUEST_TIMEOUT_MS),
  ) {
    this.log = logger.child({ component: 'worker', workerId: id });
  }

  /** One claim-and-process cycle. Exposed so tests can step the worker deterministically. */
  async runOnce(): Promise<{ processed: number; deferredForRateLimit: number }> {
    const { claimed, deferredForRateLimit } = await claimJobs(this.id);

    if (deferredForRateLimit > 0) {
      this.log.debug({ deferredForRateLimit }, 'jobs returned to queue for lack of rate-limit budget');
    }
    if (claimed.length === 0) return { processed: 0, deferredForRateLimit };

    this.log.info(
      { count: claimed.length, jobIds: claimed.map((j) => j.id) },
      'claimed jobs',
    );

    // The batch was admitted by the rate limiter as a unit, so it is safe to
    // send concurrently. Failures are per-job and never reject this Promise.
    await Promise.all(claimed.map((job) => this.process(job)));

    return { processed: claimed.length, deferredForRateLimit };
  }

  private async process(job: ClaimedJob): Promise<void> {
    const log = this.log.child({ jobId: job.id, patient: job.external_ref });

    try {
      const outcome = job.external_request_id
        ? // Already in flight at the source — check on it.
          await this.ehr.pollStatus(job.external_ref, job.external_request_id)
        : // Fresh attempt. The idempotency key is (job, attempt): stable across
          // a crash so a retried send is deduplicated by the source, but new on
          // a genuine retry so we actually get a fresh retrieval.
          await this.ehr.startRefresh(job.external_ref, `${job.id}:${job.attempts}`);

      const applied = await recordOutcome(job, outcome);
      if (!applied) {
        // Our lease lapsed and another worker legitimately took the job. Its
        // result is authoritative; ours is stale and was refused. Dropping it
        // is the correct outcome, not an error.
        log.warn({ outcome: outcome.kind }, 'lease lost before the outcome could be recorded; discarded');
        return;
      }

      if (outcome.kind === 'success') log.info('refresh completed');
      else if (outcome.kind === 'permanent') log.warn({ reason: outcome.message }, 'refresh failed permanently');
      else if (outcome.kind === 'transient') log.warn({ reason: outcome.message }, 'refresh failed transiently, will retry');
      else if (outcome.kind === 'rate_limited') log.warn({ reason: outcome.message }, 'throttled by source');
      else if (outcome.kind === 'ambiguous')
        log.warn({ reason: outcome.message }, 'send outcome unknown; will retry the same idempotency key');
      else log.debug({ outcome: outcome.kind }, 'refresh in flight');
    } catch (err) {
      // Deliberately swallowed. If we cannot even record the outcome, the lease
      // expires and another worker reclaims the job — which is exactly the
      // behaviour we want, and it is already tested by the crashed-worker case.
      log.error({ err }, 'failed to process job; leaving it to lease expiry');
    }
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.log.info('worker started');

    this.loop = (async () => {
      while (this.running) {
        try {
          const { processed } = await this.runOnce();
          if (processed === 0) await sleep(config.WORKER_IDLE_MS);
        } catch (err) {
          this.log.error({ err }, 'worker cycle failed');
          await sleep(config.WORKER_IDLE_MS);
        }
      }
    })();
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    await this.loop;

    // Hand back anything still leased so a surviving worker can take it now
    // rather than after the lease times out. Correctness does not depend on
    // this; deploy latency does.
    const released = await releaseLeases(this.id);
    this.log.info({ released }, 'worker stopped');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
