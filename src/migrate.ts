import { readFile } from 'node:fs/promises';
import { pool } from './db.js';
import { config } from './config.js';
import { logger } from './logger.js';

const log = logger.child({ component: 'migrate' });

// Resolved relative to the compiled file in dist/, so both `npm start` and
// `tsx src/main.ts` find the same SQL.
const schemaPath = new URL('../db/schema.sql', import.meta.url);
const seedPath = new URL('../db/seed.sql', import.meta.url);

/** Arbitrary but fixed: any process doing schema work takes this same lock. */
const MIGRATION_LOCK_KEY = 8_421_337;

async function tablesExist(): Promise<boolean> {
  const { rows } = await pool.query(`SELECT to_regclass('public.refresh_jobs') IS NOT NULL AS present`);
  return Boolean(rows[0]?.present);
}

/**
 * Apply schema + seed if the database is empty.
 *
 * A real service would use versioned forward-only migrations run as a deploy
 * step. This is a single-schema exercise, so create-if-absent keeps startup to
 * one command without pretending to be a migration framework.
 *
 * The advisory lock matters under docker-compose, where the scheduler and
 * several workers boot simultaneously against a fresh database. Without it they
 * would race and several would run a script whose first statement is DROP
 * TABLE. The lock serialises them; the losers find the tables already there and
 * return. Session-scoped, so a crash mid-migration releases it automatically.
 */
export async function ensureSchema(): Promise<void> {
  if (await tablesExist()) {
    log.debug('schema already present');
    return;
  }

  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_KEY]);

    // Re-check now that we hold the lock: another process may have applied the
    // schema while we were waiting for it.
    const { rows } = await client.query(`SELECT to_regclass('public.refresh_jobs') IS NOT NULL AS present`);
    if (rows[0]?.present) {
      log.debug('schema applied by another process while waiting');
      return;
    }

    log.info('empty database detected — applying schema and seed data');
    await client.query(await readFile(schemaPath, 'utf8'));
    await client.query(await readFile(seedPath, 'utf8'));

    const { rows: counts } = await client.query(
      `SELECT (SELECT count(*) FROM patients)::int        AS patients,
              (SELECT count(*) FROM studies)::int         AS studies,
              (SELECT count(*) FROM patient_studies)::int AS enrollments,
              (SELECT count(*) FROM ehr_endpoints)::int   AS endpoints`,
    );
    log.info(counts[0], 'database ready');
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY]).catch(() => {});
    client.release();
  }
}

/**
 * Guard for destructive operations.
 *
 * resetDatabase() runs a script whose first statements are DROP TABLE ...
 * CASCADE. Pointing DATABASE_URL at the wrong host is a normal human error, and
 * the cost of that error here is total data loss. So: local hosts and databases
 * that look disposable are allowed; anything else needs an explicit opt-in.
 */
export function assertSafeToReset(url: string = config.DATABASE_URL): void {
  const parsed = new URL(url);
  const database = decodeURIComponent(parsed.pathname.replace(/^\//, ''));
  const host = parsed.hostname;

  if (process.env.ALLOW_DATABASE_RESET === '1') return;

  const localHost = ['localhost', '127.0.0.1', '::1', 'postgres', 'db'].includes(host);
  // BUG 21. Only names that announce themselves as disposable are automatic.
  // `healthex` is local, but it is the database someone is demoing from — it
  // needs the same deliberate opt-in as anything else. npm run demo/db:reset
  // set ALLOW_DATABASE_RESET=1 explicitly, which is the intent being declared.
  const disposableName = /(_test|_dev)$/.test(database);

  if (!localHost || !disposableName) {
    throw new Error(
      `Refusing to reset ${database} at ${host}: this drops every table. ` +
        `Only local hosts and databases matching *_test / *_dev / healthex are allowed. ` +
        `Set ALLOW_DATABASE_RESET=1 to override deliberately.`,
    );
  }
}

/** Unconditional rebuild. Used by `npm run db:reset` and by the test harness. */
export async function resetDatabase(): Promise<void> {
  assertSafeToReset();
  const target = new URL(config.DATABASE_URL);
  log.info({ host: target.hostname, database: target.pathname.slice(1) }, 'resetting database');
  await pool.query(await readFile(schemaPath, 'utf8'));
  await pool.query(await readFile(seedPath, 'utf8'));
}
