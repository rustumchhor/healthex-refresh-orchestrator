import type { RefreshOutcome } from '../types.js';

/**
 * The orchestrator's window onto the external world.
 *
 * Its whole job is translation: turn HTTP status codes, response bodies and
 * network errors into exactly one `RefreshOutcome`. Nothing downstream of this
 * file knows what a 503 is, which is what keeps the retry policy readable and
 * testable without a network.
 *
 * The transient/permanent split is the important one, because it decides
 * whether we spend money retrying:
 *   5xx, timeouts, connection resets -> transient. The source is unwell.
 *   4xx other than 429               -> permanent. We are asking for something
 *                                       that will never be there: unknown
 *                                       patient, revoked consent, bad request.
 */
export class EhrClient {
  constructor(
    private readonly baseUrl: string,
    private readonly timeoutMs: number,
  ) {}

  /** POST /patients/{id}/$updateData */
  async startRefresh(externalRef: string, idempotencyKey: string): Promise<RefreshOutcome> {
    const url = `${this.baseUrl}/patients/${encodeURIComponent(externalRef)}/$updateData`;
    let res: Response;
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          // Makes a replayed request free. A worker that dies after sending
          // this but before committing the response will retry with the same
          // key, and the source returns the original retrieval instead of
          // starting — and charging us for — a second one.
          'idempotency-key': idempotencyKey,
        },
        body: '{}',
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      // AMBIGUOUS, not transient. A timeout or reset does not tell us whether
      // the source received the request and started retrieving. Classifying it
      // as a definitive failure would advance `attempts`, which rotates the
      // idempotency key — destroying the one mechanism that made the retry
      // safe and paying for a duplicate retrieval.
      return { kind: 'ambiguous', message: `no answer from source: ${errorMessage(err)}` };
    }

    if (res.status === 429) {
      return {
        kind: 'rate_limited',
        retryAfterSeconds: parseRetryAfter(res.headers.get('retry-after')),
        message: 'throttled by source on $updateData',
      };
    }

    if (res.status === 202 || res.status === 200) {
      const body = (await safeJson(res)) as { requestId?: string } | null;
      if (!body?.requestId) {
        // BUG 17. The source ACCEPTED the request — retrieval has started and
        // we will be billed for it. We merely failed to read the identifier.
        // Calling that `transient` advanced `attempts`, rotated the idempotency
        // key, and bought a second retrieval. Same class as BUG 5, one branch
        // further down: an accepted send is never a failure.
        return { kind: 'ambiguous', message: `accepted (${res.status}) but no readable requestId` };
      }
      return { kind: 'accepted', requestId: body.requestId };
    }

    const body = (await safeJson(res)) as { message?: string } | null;
    const message = `$updateData ${res.status}: ${body?.message ?? 'no detail'}`;
    return res.status >= 500 ? { kind: 'transient', message } : { kind: 'permanent', message };
  }

  /** GET /patients/{id}/data-retrieval/status */
  async pollStatus(externalRef: string, requestId: string): Promise<RefreshOutcome> {
    const url =
      `${this.baseUrl}/patients/${encodeURIComponent(externalRef)}/data-retrieval/status` +
      `?requestId=${encodeURIComponent(requestId)}`;
    let res: Response;
    try {
      res = await fetch(url, { signal: AbortSignal.timeout(this.timeoutMs) });
    } catch (err) {
      // The retrieval is almost certainly still running; only our observation
      // of it failed. Retry the poll rather than restarting the refresh.
      return { kind: 'poll_deferred', message: `network error polling: ${errorMessage(err)}` };
    }

    if (res.status === 429) {
      return {
        kind: 'rate_limited',
        retryAfterSeconds: parseRetryAfter(res.headers.get('retry-after')),
        message: 'throttled by source on status poll',
      };
    }

    if (res.status === 404) {
      // BUG 18. A 404 on a read-your-writes path is not proof of absence: a
      // distributed source can legitimately 404 a retrieval it has accepted but
      // not yet replicated. Restarting from $updateData under a new key turned
      // replication lag into duplicate paid work. Poll again instead; the
      // immutable job lifetime bounds this if the retrieval really is gone.
      return { kind: 'poll_deferred', message: 'source has no record of the retrieval yet' };
    }

    if (res.status >= 500) {
      return { kind: 'poll_deferred', message: `status poll ${res.status}` };
    }

    if (!res.ok) {
      return { kind: 'permanent', message: `status poll ${res.status}` };
    }

    const body = (await safeJson(res)) as
      | { status?: string; failure?: { class?: string; message?: string } }
      | null;

    switch (body?.status) {
      case 'in-progress':
        return { kind: 'still_running' };
      case 'completed':
        return { kind: 'success' };
      case 'failed': {
        const message = body.failure?.message ?? 'retrieval failed';
        return body.failure?.class === 'permanent'
          ? { kind: 'permanent', message }
          : { kind: 'transient', message };
      }
      default:
        return { kind: 'poll_deferred', message: `unrecognised status payload: ${JSON.stringify(body)}` };
    }
  }
}

/**
 * RFC 9110 §10.2.3 permits two forms: delay-seconds, or an absolute HTTP-date.
 * Parsing only the first meant `Retry-After: Sun, 16 Aug 2026 21:00:00 GMT`
 * fell through to a 1-second retry and hammered an endpoint that had just told
 * us to back off. Untrusted, so the result is clamped.
 */
export function parseRetryAfter(header: string | null, maxSeconds = 3600, nowMs = Date.now()): number {
  if (!header) return 1;
  const trimmed = header.trim();

  if (/^\d+$/.test(trimmed)) {
    const n = Number.parseInt(trimmed, 10);
    return Math.min(maxSeconds, Math.max(1, n));
  }

  const at = Date.parse(trimmed);
  if (Number.isFinite(at)) {
    const seconds = Math.ceil((at - nowMs) / 1000);
    return Math.min(maxSeconds, Math.max(1, seconds)); // a past date means "now"
  }

  return 1;
}

async function safeJson(res: Response): Promise<unknown> {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
