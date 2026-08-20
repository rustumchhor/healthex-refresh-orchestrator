/**
 * CORE OPERATION 7A — make mock outcomes reproducible.
 * WALKTHROUGH: Patient references hash to a stable success, transient, or
 * permanent behavior so the same seed produces explainable tests and demos.
 * The outcome percentages are demo choices, not real EHR reliability claims.
 *
 * Deterministic behaviour selection for the mock EHR.
 *
 * Outcomes are derived from a hash of the patient reference rather than from
 * Math.random(), so a given seed database always produces the same mix of
 * successes and failures. Reproducibility matters more here than realism: a
 * flaky mock makes every test flaky.
 */

export type BehaviorKind = 'success' | 'transient' | 'permanent' | 'rate_limited';

export interface Behavior {
  kind: BehaviorKind;
  /** Whether the simulated failure surfaces on $updateData or during retrieval. */
  failAt: 'post' | 'retrieval';
  /** For `transient`: how many attempts fail before one succeeds. */
  transientFailures: number;
  /** How long the simulated asynchronous retrieval takes. */
  retrievalMs: number;
}

/** FNV-1a. Small, stable across runs and processes, and not Math.random(). */
export function hashString(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/**
 * Default population mix: ~80% clean, ~12% transiently flaky, ~8% permanently
 * broken (unknown patient, revoked consent). `rate_limited` is deliberately not
 * in this distribution — it is produced by the mock's real token bucket, so
 * throttling is observed rather than faked.
 */
export function defaultBehavior(externalRef: string): Behavior {
  const h = hashString(externalRef);
  const bucket = h % 100;

  const kind: BehaviorKind = bucket < 80 ? 'success' : bucket < 92 ? 'transient' : 'permanent';

  return {
    kind,
    // Alternate where the failure surfaces so both code paths stay exercised:
    // a rejected $updateData and an accepted-then-failed retrieval are handled
    // by different branches in the orchestrator.
    failAt: (h >>> 8) % 2 === 0 ? 'post' : 'retrieval',
    transientFailures: 1 + ((h >>> 16) % 2), // 1 or 2 failures, then success
    retrievalMs: 100 + (h % 400),
  };
}
