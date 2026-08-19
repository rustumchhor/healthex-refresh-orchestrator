import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import { pool } from './db.js';
import { scheduleRefreshBatch } from './domain/schedule.js';
import { checkEligibility, findDuePatients } from './domain/eligibility.js';

/**
 * Admin API.
 *
 * Small on purpose. It exists for two reasons: to expose the user-triggered
 * refresh path (which is where job priority stops being theoretical), and to
 * make the system observable enough to demo without attaching to psql.
 */
export function buildApi(): FastifyInstance {
  const app = Fastify({ logger: false });

  // Route annotations are compile-time only. Without runtime parsing, `NaN`
  // reached a bigint parameter and PostgreSQL became the request validator —
  // returning 500 and leaking SQLSTATE 22P02 and internal messages to callers.
  const PositiveInt = z.coerce.number().int().positive();

  app.setErrorHandler((err: unknown, _req, reply) => {
    const status = (err as { statusCode?: number })?.statusCode ?? 500;
    if (status >= 500) {
      // Log the real cause; return nothing that describes our internals.
      // eslint-disable-next-line no-console
      console.error(JSON.stringify({ level: 50, component: 'api', msg: 'request failed', err: String(err) }));
      reply.code(500);
      return { error: 'internal_error' };
    }
    reply.code(status);
    return { error: 'bad_request', message: (err as Error)?.message ?? 'invalid request' };
  });

  /** Liveness: is this process up? Deliberately does not touch the database. */
  app.get('/health', async () => ({ ok: true }));

  /**
   * Readiness: can we actually serve? A static 200 that reports healthy while
   * PostgreSQL is unreachable is worse than no check at all — it keeps a broken
   * instance in the load balancer.
   */
  app.get('/ready', async (_req, reply) => {
    try {
      await pool.query('SELECT 1');
      return { ok: true, database: 'up' };
    } catch (err) {
      // Log the cause; tell the caller only that we are not ready. A readiness
      // probe is frequently unauthenticated, so leaking SQLSTATE, host names or
      // driver internals here is a disclosure with no upside.
      // eslint-disable-next-line no-console
      console.error(JSON.stringify({ level: 50, component: 'api', msg: 'readiness check failed', err: String(err) }));
      reply.code(503);
      return { ok: false, database: 'down' };
    }
  });

  // -- Core operation 1, exposed ------------------------------------------
  app.get<{ Params: { patientId: string; studyId: string } }>(
    '/patients/:patientId/studies/:studyId/eligibility',
    async (req, reply) => {
      const ids = z
        .object({ patientId: PositiveInt, studyId: PositiveInt })
        .safeParse(req.params);
      if (!ids.success) {
        reply.code(400);
        return { error: 'bad_request', issues: ids.error.issues };
      }

      const result = await checkEligibility(ids.data.patientId, ids.data.studyId);
      if (!result) {
        reply.code(404);
        return { error: 'not_found', message: 'no such enrollment' };
      }
      return result;
    },
  );

  app.get('/due', async () => {
    const due = await findDuePatients({ limit: 100 });
    return { count: due.length, patients: due };
  });

  // -- Core operation 2, exposed ------------------------------------------
  // "user-triggered" from the exercise's priority examples: a human asked for
  // this patient now, so it jumps the queue and skips load-spreading jitter.
  const RefreshBody = z.object({
    patientIds: z.array(z.number().int().positive()).min(1).max(1000),
    priority: z.number().int().min(0).max(100000).default(1000),
  });

  app.post('/refresh', async (req, reply) => {
    const parsed = RefreshBody.safeParse(req.body);
    if (!parsed.success) {
      reply.code(400);
      return { error: 'bad_request', issues: parsed.error.issues };
    }

    const result = await scheduleRefreshBatch({
      patientIds: parsed.data.patientIds,
      priorityOverride: parsed.data.priority,
    });

    return {
      requested: parsed.data.patientIds.length,
      scheduled: result.scheduled.length,
      // Already had a refresh in flight. Not an error — it is the duplicate
      // prevention rule doing its job, and the caller should be told so.
      alreadyInFlight: result.skippedDuplicates,
      jobs: result.scheduled,
    };
  });

  // -- Observability -------------------------------------------------------
  app.get('/stats', async () => {
    const [jobs, buckets, studies] = await Promise.all([
      pool.query(
        `SELECT status, failure_class, count(*)::int AS count
           FROM refresh_jobs GROUP BY status, failure_class ORDER BY status`,
      ),
      pool.query(
        `SELECT e.key,
                e.rate_limit_per_min,
                round(b.tokens::numeric, 2)::float8 AS tokens,
                b.capacity
           FROM rate_limit_buckets b
           JOIN ehr_endpoints e ON e.id = b.endpoint_id
          ORDER BY e.key`,
      ),
      pool.query(
        `SELECT s.name,
                count(*)::int                                        AS enrollments,
                count(*) FILTER (WHERE ps.last_refresh_at IS NULL)::int AS never_refreshed
           FROM patient_studies ps
           JOIN studies s ON s.id = ps.study_id
          GROUP BY s.name ORDER BY s.name`,
      ),
    ]);

    return { jobs: jobs.rows, rateLimits: buckets.rows, studies: studies.rows };
  });

  const JobsQuery = z.object({
    status: z.enum(['pending', 'in_progress', 'completed', 'failed']).optional(),
    limit: z.coerce.number().int().min(1).max(500).default(50),
  });

  app.get<{ Querystring: { status?: string; limit?: string } }>('/jobs', async (req, reply) => {
    const q = JobsQuery.safeParse(req.query);
    if (!q.success) {
      reply.code(400);
      return { error: 'bad_request', issues: q.error.issues };
    }
    const { limit, status } = q.data;
    const { rows } = await pool.query(
      `SELECT j.id, j.patient_id, p.external_ref, j.status, j.priority, j.attempts,
              j.coalesced_study_ids, j.run_at, j.locked_by, j.lease_expires_at,
              j.external_request_id, j.failure_class, j.last_error, j.finished_at
         FROM refresh_jobs j
         JOIN patients p ON p.id = j.patient_id
        WHERE ($1::text IS NULL OR j.status = $1)
        ORDER BY j.updated_at DESC
        LIMIT $2`,
      [status ?? null, limit],
    );
    return { count: rows.length, jobs: rows };
  });

  return app;
}
