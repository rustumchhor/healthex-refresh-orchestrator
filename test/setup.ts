import { afterAll } from 'vitest';
import pg from 'pg';
import { config } from '../src/config.js';
import { closePool } from '../src/db.js';

/**
 * Test bootstrap, run once per test file before anything else.
 *
 * 1. Creates the test database if it does not exist.
 *
 *    Without this, a fresh checkout produces dozens of identical
 *    `database "healthex_test" does not exist` failures whose real cause is one
 *    forgotten `createdb`. That noise buries genuine breakage, and "run these
 *    two commands in the right order first" is a bad contract for a test suite.
 *
 * 2. Owns pool teardown for the whole file.
 *
 *    Individual suites must NOT close the pool themselves: a file with several
 *    `describe` blocks would call it once per block, and the second call throws
 *    "Called end on pool more than once".
 */

const url = new URL(config.DATABASE_URL);
const databaseName = decodeURIComponent(url.pathname.replace(/^\//, ''));

// CREATE DATABASE cannot take a bound parameter, so the name is interpolated.
// Validate it rather than trusting the connection string.
if (!/^[A-Za-z0-9_-]+$/.test(databaseName)) {
  throw new Error(`Refusing to auto-create a database with an unexpected name: ${databaseName}`);
}

// Connect to the maintenance database on the same server to do the creating.
const adminUrl = new URL(url.toString());
adminUrl.pathname = '/postgres';

const admin = new pg.Client({ connectionString: adminUrl.toString() });
await admin.connect();
try {
  const { rowCount } = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [databaseName]);
  if (!rowCount) {
    await admin.query(`CREATE DATABASE "${databaseName}"`);
  }
} catch (err) {
  // 42P04 = duplicate_database: another test file won the race. Harmless.
  if ((err as { code?: string }).code !== '42P04') throw err;
} finally {
  await admin.end();
}

afterAll(async () => {
  await closePool();
});
