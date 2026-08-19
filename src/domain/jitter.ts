/**
 * Deterministic spreading.
 *
 * "Distribution: spread load over time to avoid thundering herd" is a
 * requirement, and the obvious implementation — Math.random() — makes the
 * system impossible to test and moves a patient to a different slot every
 * cycle. Hashing the patient id instead gives us both properties for free:
 * load is spread, and patient 41 lands in the same slot every day, which makes
 * a scheduling bug reproducible rather than intermittent.
 */

export function hashInt(n: number, salt = 0): number {
  let h = (0x811c9dc5 ^ salt) >>> 0;
  let v = n >>> 0;
  for (let i = 0; i < 4; i++) {
    h ^= v & 0xff;
    h = Math.imul(h, 0x01000193) >>> 0;
    v >>>= 8;
  }
  return h >>> 0;
}

/** A stable value in [0, 1). */
export function unitHash(n: number, salt = 0): number {
  return hashInt(n, salt) / 0x100000000;
}

/**
 * Where inside the jitter window this patient's refresh lands.
 *
 * The window is a fraction of the tightest interval that came due, capped by
 * config, so a two-minute study is spread over seconds while a daily study is
 * spread over an hour. Jitter that ignored the interval would either be
 * useless for daily work or would breach the freshness target for frequent work.
 */
export function jitterSeconds(patientId: number, tightestIntervalSeconds: number, fraction: number, maxSeconds: number): number {
  const window = Math.min(maxSeconds, tightestIntervalSeconds * fraction);
  if (window <= 0) return 0;
  return unitHash(patientId, 0x5eed) * window;
}
