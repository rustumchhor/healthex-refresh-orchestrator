import type { FastifyInstance } from 'fastify';
import { pool } from '../src/db.js';
import { resetDatabase } from '../src/migrate.js';
import { buildMockEhr } from '../src/mock-ehr/server.js';
import { EhrClient } from '../src/ehr/client.js';
import type { RefreshJob } from '../src/types.js';

export async function resetDb(): Promise<void> {
  await resetDatabase();
}

export interface MockHarness {
  baseUrl: string;
  client: EhrClient;
  setBehavior(externalRef: string, patch: Record<string, unknown>): Promise<void>;
  setRateLimit(perMinute: number): Promise<void>;
  reset(): Promise<void>;
  close(): Promise<void>;
}

/** Mock EHR on an ephemeral port, so test files never fight over one. */
export async function startMockEhr(rateLimitPerMin = 100_000): Promise<MockHarness> {
  const app: FastifyInstance = buildMockEhr({ rateLimitPerMin });
  await app.listen({ port: 0, host: '127.0.0.1' });

  const address = app.server.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  const baseUrl = `http://127.0.0.1:${port}`;

  const post = async (path: string, body: unknown): Promise<void> => {
    const res = await fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`${path} failed: ${res.status} ${await res.text()}`);
  };

  return {
    baseUrl,
    client: new EhrClient(baseUrl, 5_000),
    setBehavior: (externalRef, patch) => post('/_control/behavior', { externalRef, ...patch }),
    setRateLimit: (perMinute) => post('/_control/rate-limit', { perMinute }),
    reset: () => post('/_control/reset', {}),
    close: () => app.close(),
  };
}

// --- small query helpers so tests read as assertions, not as SQL -------------

export async function patientId(externalRef: string): Promise<number> {
  const { rows } = await pool.query(`SELECT id FROM patients WHERE external_ref = $1`, [externalRef]);
  if (!rows[0]) throw new Error(`no patient ${externalRef}`);
  return rows[0].id as number;
}

export async function studyId(name: string): Promise<number> {
  const { rows } = await pool.query(`SELECT id FROM studies WHERE name = $1`, [name]);
  if (!rows[0]) throw new Error(`no study ${name}`);
  return rows[0].id as number;
}

export async function getJob(id: number): Promise<RefreshJob> {
  const { rows } = await pool.query(`SELECT * FROM refresh_jobs WHERE id = $1`, [id]);
  if (!rows[0]) throw new Error(`no job ${id}`);
  return rows[0] as RefreshJob;
}

export async function jobForPatient(pid: number): Promise<RefreshJob | null> {
  const { rows } = await pool.query(
    `SELECT * FROM refresh_jobs WHERE patient_id = $1 ORDER BY id DESC LIMIT 1`,
    [pid],
  );
  return (rows[0] as RefreshJob) ?? null;
}

/** Pretend the last refresh happened `interval` ago (or never, with null). */
export async function setLastRefresh(pid: number, sid: number, interval: string | null): Promise<void> {
  await pool.query(
    `UPDATE patient_studies
        SET last_refresh_at = CASE WHEN $3::text IS NULL THEN NULL ELSE now() - $3::interval END
      WHERE patient_id = $1 AND study_id = $2`,
    [pid, sid, interval],
  );
}

/** Simulate a worker that died: its lease lapses without the job finishing. */
export async function expireLease(jobId: number): Promise<void> {
  await pool.query(`UPDATE refresh_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, [jobId]);
}

export async function setTokens(endpointKey: string, tokens: number): Promise<void> {
  await pool.query(
    `UPDATE rate_limit_buckets b
        SET tokens = $2, last_refill_at = now()
       FROM ehr_endpoints e
      WHERE e.id = b.endpoint_id AND e.key = $1`,
    [endpointKey, tokens],
  );
}

export async function getTokens(endpointKey: string): Promise<number> {
  const { rows } = await pool.query(
    `SELECT b.tokens FROM rate_limit_buckets b JOIN ehr_endpoints e ON e.id = b.endpoint_id WHERE e.key = $1`,
    [endpointKey],
  );
  return Number(rows[0]?.tokens ?? 0);
}

/** Remove every enrollment except the named one, so a test population is exact. */
export async function isolatePatients(patientIds: number[]): Promise<void> {
  await pool.query(`DELETE FROM patient_studies WHERE NOT (patient_id = ANY($1::bigint[]))`, [patientIds]);
}

export async function insertJob(overrides: {
  patientId: number;
  studyIds?: number[];
  priority?: number;
  runAtOffsetSeconds?: number;
  deadlineOffsetSeconds?: number;
  maxAttempts?: number;
}): Promise<RefreshJob> {
  const { rows } = await pool.query(
    `INSERT INTO refresh_jobs
        (patient_id, coalesced_study_ids, status, priority, run_at, deadline_at, max_attempts)
     VALUES ($1, $2::bigint[], 'pending', $3,
             now() + make_interval(secs => $4::float8),
             now() + make_interval(secs => $5::float8),
             $6)
     RETURNING *`,
    [
      overrides.patientId,
      overrides.studyIds ?? [],
      overrides.priority ?? 100,
      overrides.runAtOffsetSeconds ?? 0,
      overrides.deadlineOffsetSeconds ?? 300,
      overrides.maxAttempts ?? 3,
    ],
  );
  return rows[0] as RefreshJob;
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
