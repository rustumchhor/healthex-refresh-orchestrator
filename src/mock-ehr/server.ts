import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import { defaultBehavior, hashString, type Behavior, type BehaviorKind } from './behavior.js';

/**
 * Mock of the external HealthEx/EHR API.
 *
 * This is the ONLY simulated component in the project. It implements exactly
 * the two routes named in the exercise and invents nothing else:
 *
 *   POST /patients/{id}/$updateData              -> starts a data retrieval
 *   GET  /patients/{id}/data-retrieval/status    -> reports on it
 *
 * $updateData is asynchronous: it returns 202 with a request id, and the
 * retrieval completes some time later. That matches how a real data exchange
 * behaves — pulling records from Epic is not a request/response affair — and it
 * is what makes the orchestrator's "release the job between polls" behaviour
 * meaningful.
 *
 * The four response classes the exercise asks for are all reachable:
 *   success          202 -> poll -> completed
 *   transient        503 on POST, or poll -> failed/transient
 *   permanent        422 on POST, or poll -> failed/permanent
 *   rate limited     429 + Retry-After, from a real token bucket
 */

interface Retrieval {
  requestId: string;
  externalRef: string;
  createdAtMs: number;
  completesAtMs: number;
  terminal: 'success' | 'transient' | 'permanent';
}

/** Simple in-memory token bucket. The mock throttling *us*, not us throttling it. */
class TokenBucket {
  private tokens: number;
  private lastRefillMs: number;

  constructor(
    private capacity: number,
    private refillPerSec: number,
  ) {
    this.tokens = capacity;
    this.lastRefillMs = Date.now();
  }

  reconfigure(capacity: number, refillPerSec: number): void {
    this.capacity = capacity;
    this.refillPerSec = refillPerSec;
    this.tokens = capacity;
    this.lastRefillMs = Date.now();
  }

  /** Returns null when allowed, or the seconds to wait when throttled. */
  take(): number | null {
    const now = Date.now();
    this.tokens = Math.min(this.capacity, this.tokens + ((now - this.lastRefillMs) / 1000) * this.refillPerSec);
    this.lastRefillMs = now;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return null;
    }
    return Math.max(1, Math.ceil((1 - this.tokens) / this.refillPerSec));
  }
}

export interface MockEhrOptions {
  rateLimitPerMin: number;
  logger?: boolean;
}

export function buildMockEhr(opts: MockEhrOptions): FastifyInstance {
  const app = Fastify({ logger: opts.logger ?? false });

  /** Retrieval keyed by idempotency key, so a replayed POST is not new work. */
  const retrievalsByKey = new Map<string, Retrieval>();
  /** Newest retrieval per patient — how the status route answers without a request id. */
  const latestByPatient = new Map<string, Retrieval>();
  /** How many times each patient's refresh has been attempted, for flaky patients. */
  const attemptsByPatient = new Map<string, number>();
  /** Test overrides, checked before the deterministic default. */
  const overrides = new Map<string, Partial<Behavior>>();

  let bucket = new TokenBucket(opts.rateLimitPerMin, opts.rateLimitPerMin / 60);

  function behaviorFor(externalRef: string): Behavior {
    return { ...defaultBehavior(externalRef), ...(overrides.get(externalRef) ?? {}) };
  }

  /** Every route funnels through here first: throttling applies to all API calls. */
  function throttled(reply: { code: (n: number) => { header: (k: string, v: string) => unknown } }): number | null {
    const wait = bucket.take();
    if (wait !== null) reply.code(429).header('Retry-After', String(wait));
    return wait;
  }

  // -------------------------------------------------------------------------
  // POST /patients/{id}/$updateData
  // -------------------------------------------------------------------------
  app.post<{ Params: { id: string }; Headers: { 'idempotency-key'?: string } }>(
    '/patients/:id/$updateData',
    async (req, reply) => {
      const externalRef = req.params.id;

      const wait = throttled(reply);
      if (wait !== null) {
        return { error: 'rate_limited', message: `Too many requests; retry in ${wait}s`, retryAfterSeconds: wait };
      }

      const behavior = behaviorFor(externalRef);

      if (behavior.kind === 'rate_limited') {
        reply.code(429).header('Retry-After', '2');
        return { error: 'rate_limited', message: 'Endpoint quota exhausted', retryAfterSeconds: 2 };
      }

      // Replay protection. A worker that crashed after sending this request but
      // before recording the response will retry with the same key; returning
      // the original retrieval means we do not pay for the same pull twice.
      const idempotencyKey = req.headers['idempotency-key'] ?? `${externalRef}:${Date.now()}`;
      const existing = retrievalsByKey.get(idempotencyKey);
      if (existing) {
        reply.code(202);
        return { requestId: existing.requestId, status: 'in-progress', replayed: true };
      }

      const attemptNo = (attemptsByPatient.get(externalRef) ?? 0) + 1;
      attemptsByPatient.set(externalRef, attemptNo);

      const stillFailing = behavior.kind === 'transient' && attemptNo <= behavior.transientFailures;

      if (behavior.failAt === 'post') {
        if (behavior.kind === 'permanent') {
          reply.code(422);
          return { error: 'permanent', message: 'Patient not found at source or consent revoked' };
        }
        if (stillFailing) {
          reply.code(503);
          return { error: 'transient', message: `Upstream EHR unavailable (attempt ${attemptNo})` };
        }
      }

      const terminal: Retrieval['terminal'] =
        behavior.failAt === 'retrieval' && behavior.kind === 'permanent'
          ? 'permanent'
          : behavior.failAt === 'retrieval' && stillFailing
            ? 'transient'
            : 'success';

      const now = Date.now();
      const retrieval: Retrieval = {
        requestId: `req-${hashString(idempotencyKey).toString(16)}`,
        externalRef,
        createdAtMs: now,
        completesAtMs: now + behavior.retrievalMs,
        terminal,
      };
      retrievalsByKey.set(idempotencyKey, retrieval);
      latestByPatient.set(externalRef, retrieval);

      reply.code(202);
      return { requestId: retrieval.requestId, status: 'in-progress' };
    },
  );

  // -------------------------------------------------------------------------
  // GET /patients/{id}/data-retrieval/status
  //
  // requestId is accepted but optional. The orchestrator guarantees at most one
  // active refresh per patient, so "the latest retrieval" is unambiguous.
  // -------------------------------------------------------------------------
  app.get<{ Params: { id: string }; Querystring: { requestId?: string } }>(
    '/patients/:id/data-retrieval/status',
    async (req, reply) => {
      const externalRef = req.params.id;

      const wait = throttled(reply);
      if (wait !== null) {
        return { error: 'rate_limited', message: `Too many requests; retry in ${wait}s`, retryAfterSeconds: wait };
      }

      const retrieval = latestByPatient.get(externalRef);
      if (!retrieval || (req.query.requestId && req.query.requestId !== retrieval.requestId)) {
        reply.code(404);
        return { error: 'not_found', message: 'No data retrieval for this patient' };
      }

      if (Date.now() < retrieval.completesAtMs) {
        return { requestId: retrieval.requestId, status: 'in-progress' };
      }

      if (retrieval.terminal === 'success') {
        return {
          requestId: retrieval.requestId,
          status: 'completed',
          completedAt: new Date(retrieval.completesAtMs).toISOString(),
        };
      }

      return {
        requestId: retrieval.requestId,
        status: 'failed',
        failure: {
          class: retrieval.terminal,
          message:
            retrieval.terminal === 'permanent'
              ? 'Source rejected the request permanently'
              : 'Source timed out during retrieval',
        },
      };
    },
  );

  // -------------------------------------------------------------------------
  // Test control surface. Not part of the simulated contract — it exists so
  // tests can pin a patient to an exact outcome instead of hunting for a
  // patient whose hash happens to produce it.
  // -------------------------------------------------------------------------
  const BehaviorPatch = z.object({
    externalRef: z.string(),
    kind: z.enum(['success', 'transient', 'permanent', 'rate_limited']).optional(),
    failAt: z.enum(['post', 'retrieval']).optional(),
    transientFailures: z.number().int().min(0).optional(),
    retrievalMs: z.number().int().min(0).optional(),
  });

  app.post('/_control/behavior', async (req, reply) => {
    const parsed = BehaviorPatch.safeParse(req.body);
    if (!parsed.success) {
      reply.code(400);
      return { error: 'bad_request', issues: parsed.error.issues };
    }
    const { externalRef, ...patch } = parsed.data;
    overrides.set(externalRef, { ...(overrides.get(externalRef) ?? {}), ...(patch as Partial<Behavior>) });
    return { ok: true, externalRef, behavior: behaviorFor(externalRef) as { kind: BehaviorKind } };
  });

  app.post('/_control/rate-limit', async (req) => {
    const { perMinute } = z.object({ perMinute: z.number().min(0.1) }).parse(req.body);
    bucket = new TokenBucket(perMinute, perMinute / 60);
    return { ok: true, perMinute };
  });

  app.post('/_control/reset', async () => {
    retrievalsByKey.clear();
    latestByPatient.clear();
    attemptsByPatient.clear();
    overrides.clear();
    bucket = new TokenBucket(opts.rateLimitPerMin, opts.rateLimitPerMin / 60);
    return { ok: true };
  });

  app.get('/health', async () => ({ ok: true, service: 'mock-ehr' }));

  return app;
}
