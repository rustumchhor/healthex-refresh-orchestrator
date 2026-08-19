import { scheduleRefreshBatch } from './domain/schedule.js';
import { failExpiredJobs } from './domain/complete.js';
import { config } from './config.js';
import { logger } from './logger.js';

/**
 * The scheduler.
 *
 * Asks "who is due?" on a timer and materialises jobs for them.
 *
 * There is deliberately no leader election and no singleton lock. Running two
 * schedulers concurrently is safe because the only thing they do is INSERT ...
 * ON CONFLICT DO NOTHING against a partial unique index: the second one's rows
 * are silently discarded. Making duplicate prevention a property of the schema
 * rather than of the deployment topology removes an entire category of
 * operational risk — nobody has to remember to run exactly one.
 */
export class Scheduler {
  private running = false;
  private loop: Promise<void> | null = null;
  private readonly log = logger.child({ component: 'scheduler' });

  /** One scheduling pass. Exposed so tests can step it deterministically. */
  async tick(): Promise<{ expired: number; scheduled: number; skippedDuplicates: number }> {
    const expired = await failExpiredJobs();
    if (expired > 0) this.log.warn({ expired }, 'failed jobs past their deadline');

    const result = await scheduleRefreshBatch();

    if (result.scheduled.length > 0) {
      this.log.info(
        {
          scheduled: result.scheduled.length,
          skippedDuplicates: result.skippedDuplicates,
          topPriority: result.scheduled[0]?.priority,
        },
        'scheduled refresh jobs',
      );
    }

    return { expired, scheduled: result.scheduled.length, skippedDuplicates: result.skippedDuplicates };
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.log.info({ tickMs: config.SCHEDULER_TICK_MS }, 'scheduler started');

    this.loop = (async () => {
      while (this.running) {
        try {
          await this.tick();
        } catch (err) {
          this.log.error({ err }, 'scheduler tick failed');
        }
        await sleep(config.SCHEDULER_TICK_MS);
      }
    })();
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    await this.loop;
    this.log.info('scheduler stopped');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
