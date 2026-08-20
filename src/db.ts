import pg from 'pg';
import type { PoolClient } from 'pg';
import { config } from './config.js';

/**
 * CORE OPERATION 6C — provide the shared transaction boundary.
 * WALKTHROUGH: Database time coordinates leases and scheduling. The
 * withTransaction() helper makes claim + token spend and completion +
 * enrollment freshness atomic.
 */

const { Pool, types } = pg;

// node-postgres returns bigint (int8) as a string to avoid precision loss.
// Our identifiers are comfortably inside Number.MAX_SAFE_INTEGER, and having
// ids silently change type between a query and a test assertion is a far more
// likely bug than overflowing 2^53 patients.
types.setTypeParser(20, (v: string) => Number.parseInt(v, 10)); // int8

// int8[] (oid 1016), which is how coalesced_study_ids comes back. Without this
// pg hands us an array of strings and the ids stop comparing equal to numbers.
// @types/pg's TypeId union only enumerates scalar builtins, so the array oid
// needs a cast to be expressible.
const INT8_ARRAY_OID = 1016 as unknown as Parameters<typeof types.setTypeParser>[0];
types.setTypeParser(INT8_ARRAY_OID, (v: string) =>
  v === '{}' ? [] : v.slice(1, -1).split(',').map((n) => Number.parseInt(n, 10)),
);

export const pool = new Pool({
  connectionString: config.DATABASE_URL,
  max: 10,
  // Every wait in this system must be bounded. Claim transactions are short by
  // design, so a long wait means something is wrong and failing fast beats
  // holding a scarce pool connection plus a set of provisional row locks.
  connectionTimeoutMillis: 5_000,
  idleTimeoutMillis: 30_000,
  statement_timeout: 15_000,
  query_timeout: 15_000,
  idle_in_transaction_session_timeout: 30_000,
  // SKIP LOCKED covers the job rows, NOT the rate_limit_buckets updates that
  // follow. Without lock_timeout a worker can block indefinitely on a hot or
  // abandoned bucket row.
  options: '-c lock_timeout=5000',
});

// node-postgres emits errors on behalf of *idle* clients during backend
// restarts and network partitions. An unhandled EventEmitter 'error' takes the
// process down, so this listener is not optional.
pool.on('error', (err) => {
  // eslint-disable-next-line no-console
  console.error(JSON.stringify({ level: 50, component: 'db', msg: 'idle client error', err: String(err) }));
});

export type Queryable = Pick<PoolClient, 'query'>;

/**
 * Run `fn` inside a transaction, rolling back on any throw.
 *
 * Everything that must be atomic in this system — claiming a job together with
 * spending its rate-limit tokens, or completing a job together with advancing
 * every active enrollment timestamp for that patient — goes through here.
 */
export async function withTransaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

let poolClosed = false;

/** Idempotent: pg throws "Called end on pool more than once" on a second call. */
export async function closePool(): Promise<void> {
  if (poolClosed) return;
  poolClosed = true;
  await pool.end();
}
