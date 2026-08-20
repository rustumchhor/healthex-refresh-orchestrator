import { config, type Role } from './config.js';
import { logger } from './logger.js';
import { closePool } from './db.js';
import { ensureSchema } from './migrate.js';
import { Scheduler } from './scheduler.js';
import { Worker } from './worker.js';
import { buildApi } from './api.js';
import { buildMockEhr } from './mock-ehr/server.js';

/**
 * CORE OPERATION 6 — assemble runtime roles and manage their lifecycle.
 * WALKTHROUGH: This is a presentation label. ROLE=all runs the local system;
 * Compose selects scheduler/api, worker, and mock-ehr roles independently from
 * the same executable. Workers remain horizontally scalable because durable
 * coordination lives in PostgreSQL.
 *
 * SEARCHABLE WALKTHROUGH INDEX:
 *   0     schema and deterministic seed
 *   1     eligibility
 *   2     scheduling (2A jitter, 2B scheduler loop)
 *   3     atomic claim/rate admission (3A worker, 3B EHR, 3C outcomes)
 *   4     completion and retry state machine
 *   5     admin API and observability
 *   6     runtime, configuration, migration, and transaction setup
 *   7     deterministic mock EHR boundary
 *   8     end-to-end guided demo
 *
 * Entry point.
 *
 * ROLE decides what this process is. `all` runs everything in one process,
 * which is what `npm start` uses so the whole system is one command. The
 * individual roles exist so docker-compose can run the scheduler, the workers
 * and the mock EHR as separate containers — the same code, deployed the way it
 * actually would be, with workers scaled independently.
 */
async function main(): Promise<void> {
  const roles = new Set(config.ROLE);
  const has = (r: Role): boolean => roles.has('all') || roles.has(r);

  const log = logger.child({ roles: [...roles] });
  const shutdown: Array<() => Promise<void>> = [];

  // The mock EHR is the only role that does not touch our database.
  if (has('scheduler') || has('worker') || has('api')) await ensureSchema();

  if (has('mock-ehr')) {
    const mock = buildMockEhr({ rateLimitPerMin: config.MOCK_RATE_LIMIT_PER_MIN });
    await mock.listen({ port: config.MOCK_EHR_PORT, host: '0.0.0.0' });
    log.info({ port: config.MOCK_EHR_PORT, limitPerMin: config.MOCK_RATE_LIMIT_PER_MIN }, 'mock EHR listening');
    shutdown.push(() => mock.close());
  }

  if (has('api')) {
    const api = buildApi();
    await api.listen({ port: config.ADMIN_PORT, host: '0.0.0.0' });
    log.info({ port: config.ADMIN_PORT }, 'admin API listening');
    shutdown.push(() => api.close());
  }

  if (has('scheduler')) {
    const scheduler = new Scheduler();
    scheduler.start();
    shutdown.push(() => scheduler.stop());
  }

  if (has('worker')) {
    const suffix = `${process.env.HOSTNAME ?? 'local'}-${process.pid}`;
    const workers = Array.from({ length: config.WORKER_COUNT }, (_, i) => new Worker(`worker-${suffix}-${i}`));
    for (const w of workers) w.start();
    // Stop workers in parallel: each one waits out its current HTTP call, and
    // serialising that would multiply shutdown time by the worker count.
    shutdown.push(async () => {
      await Promise.all(workers.map((w) => w.stop()));
    });
  }

  let shuttingDown = false;
  const onSignal = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log.info({ signal }, 'shutting down');

    void (async () => {
      // Reverse order: stop producing work before tearing down what consumes it.
      for (const fn of shutdown.reverse()) {
        await fn().catch((err) => log.error({ err }, 'shutdown step failed'));
      }
      await closePool().catch(() => {});
      log.info('shutdown complete');
      process.exit(0);
    })();
  };

  process.on('SIGINT', () => onSignal('SIGINT'));
  process.on('SIGTERM', () => onSignal('SIGTERM'));
}

main().catch((err) => {
  logger.error({ err }, 'fatal error during startup');
  process.exit(1);
});
