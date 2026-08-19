import { defineConfig } from 'vitest/config';

/**
 * Tests run against a real Postgres, not a mock.
 *
 * Every interesting property in this system — SKIP LOCKED, the partial unique
 * index, transactional token spend — is a property of the database. Faking it
 * would test the fake. The tradeoff is that tests need a live server, which is
 * why `docker compose up postgres` is step one of running them.
 *
 * Durations are compressed rather than faked. The authoritative clock here is
 * Postgres `now()`, which no JS fake-timer can move, so a fake clock would
 * quietly desynchronise the app from the database.
 */
export default defineConfig({
  test: {
    env: {
      DATABASE_URL:
        process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/healthex_test',
      LOG_LEVEL: 'silent',
      // The suite rebuilds healthex_test, which ends in _test and is allowed.
      ALLOW_DATABASE_RESET: '',
      ROLE: 'all',

      LEASE_SECONDS: '2',
      // Must stay below LEASE_SECONDS — the boot-time coherence check enforces it.
      EHR_REQUEST_TIMEOUT_MS: '1000',
      POLL_INTERVAL_SECONDS: '0.05',
      RETRY_BASE_SECONDS: '0.1',
      RETRY_MAX_SECONDS: '2',
      RATE_LIMIT_COOLDOWN_SECONDS: '0.1',
      JOB_DEADLINE_SECONDS: '30',
      MAX_ATTEMPTS: '3',
      WORKER_BATCH_SIZE: '5',
      SCHEDULER_BATCH_LIMIT: '500',

      // Compressed, but non-zero: a zero window would silently disable the
      // quarantine and let the "permanent failures reschedule forever" bug back
      // in without any test noticing.
      TRANSIENT_QUARANTINE_SECONDS: '1',
      PERMANENT_QUARANTINE_SECONDS: '60',

      // No jitter, so a scheduled job is immediately claimable. Jitter is a
      // pure function and is unit-tested directly instead.
      JITTER_FRACTION: '0',
      JITTER_MAX_SECONDS: '0',
    },
    // Creates the test database if missing and owns pool teardown.
    setupFiles: ['./test/setup.ts'],
    // One shared database: files must not run concurrently.
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
