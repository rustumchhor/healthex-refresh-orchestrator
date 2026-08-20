import { z } from 'zod';

/**
 * CORE OPERATION 6A — validate runtime configuration before startup.
 * WALKTHROUGH: Schema defaults live here. assertCoherent() protects cross-value
 * safety rules such as lease > HTTP timeout and a deadline long enough for the
 * configured retry schedule.
 */

// Load .env when one exists, using Node's built-in parser — no dotenv
// dependency, and it works identically under `node`, `tsx` and `vitest`
// rather than relying on a CLI flag that only `npm start` would pass.
//
// Node's loader does NOT overwrite variables that are already set, so real
// environment variables keep precedence and the file only fills gaps. An
// earlier version guarded this on DATABASE_URL being absent, which made
// configuration all-or-nothing: overriding just the database URL silently
// discarded every other value in .env.
try {
  process.loadEnvFile();
} catch {
  // No .env file. Expected in containers, where config arrives as real env vars.
}

/**
 * All tunables live here so that tests can run the same code paths at
 * millisecond timescales instead of needing a fake clock. Faking time is the
 * usual approach and it is a trap in this system: the authoritative clock is
 * Postgres `now()`, which a JS fake timer cannot move.
 */
const Schema = z.object({
  DATABASE_URL: z.string().min(1),
  // Comma-separated so one container can be e.g. "scheduler,api" without
  // needing a second process manager.
  ROLE: z
    .string()
    .default('all')
    .transform((s) => s.split(',').map((r) => r.trim()).filter(Boolean))
    .pipe(z.array(z.enum(['all', 'scheduler', 'worker', 'api', 'mock-ehr'])).min(1)),
  LOG_LEVEL: z.string().default('info'),

  WORKER_COUNT: z.coerce.number().int().min(0).default(2),
  WORKER_BATCH_SIZE: z.coerce.number().int().min(1).default(5),
  WORKER_IDLE_MS: z.coerce.number().int().min(1).default(500),
  LEASE_SECONDS: z.coerce.number().min(1).default(30),

  POLL_INTERVAL_SECONDS: z.coerce.number().min(0.05).default(1),
  JOB_DEADLINE_SECONDS: z.coerce.number().min(1).default(300),
  MAX_ATTEMPTS: z.coerce.number().int().min(1).default(4),
  RETRY_BASE_SECONDS: z.coerce.number().min(0.05).default(2),
  RETRY_MAX_SECONDS: z.coerce.number().min(1).default(300),
  RATE_LIMIT_COOLDOWN_SECONDS: z.coerce.number().min(0.05).default(1),
  // An absolute ceiling on how long a job may exist, measured from created_at,
  // which nothing in the system may extend. deadline_at is an execution budget
  // and is deliberately pushed out while a job waits for rate-limit budget;
  // without a second, immutable bound an unlucky job could live indefinitely.
  MAX_JOB_LIFETIME_SECONDS: z.coerce.number().min(1).default(3600),

  SCHEDULER_TICK_MS: z.coerce.number().int().min(50).default(2000),
  SCHEDULER_BATCH_LIMIT: z.coerce.number().int().min(1).default(500),
  JITTER_FRACTION: z.coerce.number().min(0).max(1).default(0.1),
  JITTER_MAX_SECONDS: z.coerce.number().min(0).default(3600),
  CONSENT_URGENCY_DAYS: z.coerce.number().min(0).default(7),
  CONSENT_PRIORITY_BOOST: z.coerce.number().int().min(0).default(500),

  // After a job fails terminally the patient stays stale, so the scheduler
  // would otherwise re-create the same doomed job on its very next tick. These
  // windows suppress that. Permanent failures wait far longer than exhausted
  // transient ones, but neither waits forever — consent gets re-granted and
  // patient records get corrected.
  TRANSIENT_QUARANTINE_SECONDS: z.coerce.number().min(0).default(900), // 15 min
  PERMANENT_QUARANTINE_SECONDS: z.coerce.number().min(0).default(86_400), // 24 h

  ADMIN_PORT: z.coerce.number().int().default(4000),
  MOCK_EHR_PORT: z.coerce.number().int().default(4010),
  MOCK_EHR_BASE_URL: z.string().default('http://localhost:4010'),
  // The mock's own throttle, across all patients. Defaults to the sum of the
  // seeded endpoint limits so it does not fight the demo; tests tighten it to
  // force 429s deliberately.
  MOCK_RATE_LIMIT_PER_MIN: z.coerce.number().min(1).default(160),
  EHR_REQUEST_TIMEOUT_MS: z.coerce.number().int().min(100).default(5000),
});

export type Config = z.infer<typeof Schema>;

/** One of the process roles ROLE may name. */
export type Role = Config['ROLE'][number];

/**
 * Cross-field checks.
 *
 * Every value below is individually reasonable; the failures are in the
 * *combinations*. These are exactly the bugs that unit tests miss, because a
 * unit test picks one value and never asks whether it agrees with another.
 * Failing loudly at boot beats a misclassified failure at 3am.
 */
function assertCoherent(c: Config): void {
  // Worst-case retry schedule, including the +25% jitter ceiling.
  let retrySpan = 0;
  for (let a = 1; a < c.MAX_ATTEMPTS; a++) {
    retrySpan += Math.min(c.RETRY_MAX_SECONDS, c.RETRY_BASE_SECONDS * 2 ** (a - 1)) * 1.25;
  }
  if (retrySpan > c.JOB_DEADLINE_SECONDS) {
    throw new Error(
      `Incoherent config: the retry schedule can span ${retrySpan.toFixed(0)}s but JOB_DEADLINE_SECONDS is ` +
        `${c.JOB_DEADLINE_SECONDS}s. A job would be reaped as a 'timeout' mid-retry, misreporting a transient ` +
        `failure as a deadline breach. Raise JOB_DEADLINE_SECONDS or lower MAX_ATTEMPTS/RETRY_BASE_SECONDS.`,
    );
  }

  if (c.MAX_JOB_LIFETIME_SECONDS < c.JOB_DEADLINE_SECONDS) {
    throw new Error(
      `Incoherent config: MAX_JOB_LIFETIME_SECONDS (${c.MAX_JOB_LIFETIME_SECONDS}s) must be at least ` +
        `JOB_DEADLINE_SECONDS (${c.JOB_DEADLINE_SECONDS}s), or jobs die before their execution budget starts.`,
    );
  }

  // A lease must outlive the call it covers, or a worker can still be waiting on
  // the EHR when another worker legitimately reclaims its job.
  if (c.LEASE_SECONDS * 1000 <= c.EHR_REQUEST_TIMEOUT_MS) {
    throw new Error(
      `Incoherent config: LEASE_SECONDS (${c.LEASE_SECONDS}s) must exceed EHR_REQUEST_TIMEOUT_MS ` +
        `(${c.EHR_REQUEST_TIMEOUT_MS}ms), otherwise a lease can lapse while its HTTP call is still in flight.`,
    );
  }
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = Schema.safeParse(env);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
    throw new Error(`Invalid configuration:\n${issues}`);
  }
  assertCoherent(parsed.data);
  return parsed.data;
}

export const config: Config = loadConfig();
