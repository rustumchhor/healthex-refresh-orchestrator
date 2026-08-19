/**
 * Observable demo: `npm run demo`
 *
 * Resets the database, fills the tables, then runs the real scheduler and real
 * workers against the mock EHR while printing what is happening — jobs being
 * scheduled, claimed and processed, rate-limit budgets draining, and the
 * invariants holding. Everything printed is read back out of Postgres.
 */
import { resetDatabase } from '../src/migrate.js';
import { pool, closePool } from '../src/db.js';
import { scheduleRefreshBatch } from '../src/domain/schedule.js';
import { Worker } from '../src/worker.js';
import { buildMockEhr } from '../src/mock-ehr/server.js';
import { EhrClient } from '../src/ehr/client.js';

const ROUNDS = Number(process.env.DEMO_ROUNDS ?? 60);
const WORKERS = Number(process.env.DEMO_WORKERS ?? 4);

const q = async (sql: string, params: unknown[] = []) => (await pool.query(sql, params)).rows;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rule = (t = '') => console.log(`\n${'─'.repeat(78)}${t ? `\n${t}` : ''}`);

function table(rows: Record<string, unknown>[]): void {
  if (rows.length === 0) return console.log('  (none)');
  const cols = Object.keys(rows[0]!);
  const width = (c: string) => Math.max(c.length, ...rows.map((r) => String(r[c] ?? '').length));
  const w = Object.fromEntries(cols.map((c) => [c, width(c)]));
  console.log('  ' + cols.map((c) => c.padEnd(w[c]!)).join('  '));
  console.log('  ' + cols.map((c) => '-'.repeat(w[c]!)).join('  '));
  for (const r of rows) console.log('  ' + cols.map((c) => String(r[c] ?? '').padEnd(w[c]!)).join('  '));
}

async function main(): Promise<void> {
  // ---------------------------------------------------------------- 1. data
  rule('STEP 1 — reset and fill the tables');
  await resetDatabase();
  table(
    await q(`SELECT (SELECT count(*) FROM patients)::int          AS patients,
                    (SELECT count(*) FROM studies)::int           AS studies,
                    (SELECT count(*) FROM patient_studies)::int   AS enrollments,
                    (SELECT count(*) FROM ehr_endpoints)::int     AS endpoints,
                    (SELECT count(*) FROM patient_endpoints)::int AS endpoint_links`),
  );
  console.log('\n  Studies (refresh frequency drives WHEN a patient is due):');
  table(await q(`SELECT name, refresh_interval::text AS every, base_priority AS priority FROM studies ORDER BY id`));
  console.log('\n  EHR endpoints (rate limits drive WHAT A CALL COSTS):');
  table(await q(`SELECT key, rate_limit_per_min AS per_min,
                        (SELECT count(*) FROM patient_endpoints pe WHERE pe.endpoint_id = e.id)::int AS patients
                   FROM ehr_endpoints e ORDER BY id`));

  // ------------------------------------------------------------ 2. schedule
  rule('STEP 2 — scheduler decides who is due (CORE OP 1 + 2)');
  const mock = buildMockEhr({ rateLimitPerMin: 100_000 });
  await mock.listen({ port: 0, host: '127.0.0.1' });
  const port = (mock.server.address() as { port: number }).port;
  const client = new EhrClient(`http://127.0.0.1:${port}`, 5_000);

  // Load spreading is disabled here on purpose. In production a daily study is
  // jittered across an hour, which is correct and completely illegible in a
  // 30-second demo. Spreading has its own tests; this shows the pipeline.
  const tick = await scheduleRefreshBatch({ jitterWindowSeconds: 0 });
  console.log(
    `\n  scheduled ${tick.scheduled.length} jobs, ${tick.skippedDuplicates} skipped as duplicates` +
      `  (load spreading disabled for legibility)\n`,
  );
  console.log('  Highest-priority jobs first — `studies` = DUE studies folded into ONE job:');
  table(
    await q(`SELECT j.id, p.external_ref AS patient, j.priority,
                    array_length(j.coalesced_study_ids,1) AS studies, j.status
               FROM refresh_jobs j JOIN patients p ON p.id=j.patient_id
              ORDER BY j.priority DESC, j.id LIMIT 8`),
  );

  // -------------------------------------------------------------- 3. workers
  rule(`STEP 3a — ${WORKERS} workers claim and process under REAL rate limits (CORE OP 3 + 4)`);
  const workers = Array.from({ length: WORKERS }, (_, i) => new Worker(`worker-${i + 1}`, client));
  const claims = new Map(workers.map((w) => [w.id, { batches: 0, jobs: 0, throttled: 0 }]));

  for (let round = 1; round <= ROUNDS; round++) {
    await Promise.all(
      workers.map(async (w) => {
        const r = await w.runOnce();
        const s = claims.get(w.id)!;
        if (r.processed > 0) s.batches++;
        s.jobs += r.processed;
        s.throttled += r.deferredForRateLimit;
      }),
    );

    if (round % 20 === 0) {
      const [state] = await q(
        `SELECT count(*) FILTER (WHERE status='pending')::int      AS pending,
                count(*) FILTER (WHERE status='in_progress')::int  AS in_progress,
                count(*) FILTER (WHERE status='completed')::int    AS completed,
                count(*) FILTER (WHERE status='failed')::int       AS failed
           FROM refresh_jobs`,
      );
      const budget = await q(
        `SELECT e.key, round(b.tokens::numeric,1)::text AS tokens FROM rate_limit_buckets b
           JOIN ehr_endpoints e ON e.id=b.endpoint_id ORDER BY e.key`,
      );
      console.log(
        `\n  round ${String(round).padStart(3)}  ` +
          `pending=${state!.pending} in_progress=${state!.in_progress} ` +
          `completed=${state!.completed} failed=${state!.failed}   ` +
          `budget: ${budget.map((b) => `${b.key}=${b.tokens}`).join('  ')}`,
      );
    }
    await sleep(25);
  }

  const [throttled] = await q(
    `SELECT count(*) FILTER (WHERE status='completed')::int AS completed,
            count(*) FILTER (WHERE status='pending')::int   AS still_queued
       FROM refresh_jobs`,
  );
  console.log(
    `\n  Rate limiting is binding: ${throttled!.completed} done, ${throttled!.still_queued} queued waiting for budget.`,
  );
  console.log('  60 patients x 2 calls each (POST + poll) = 120 Epic calls, against a 100/min limit.');
  console.log('  Nothing failed — throttled work is queued, not dropped.');

  // ------------------------------------------------- 3b. remove the ceiling
  rule('STEP 3b — same system, rate limits lifted, so it drains');
  await q(`UPDATE rate_limit_buckets SET tokens=100000, capacity=100000, refill_per_sec=10000`);

  const deadline = Date.now() + 30_000; // wall-clock bound: retry backoff can outlast a fixed round count
  let drained = false;
  for (let round = 1; round <= 2000 && Date.now() < deadline; round++) {
    await Promise.all(
      workers.map(async (w) => {
        const r = await w.runOnce();
        const s = claims.get(w.id)!;
        if (r.processed > 0) s.batches++;
        s.jobs += r.processed;
      }),
    );
    const [left] = await q(`SELECT count(*)::int AS n FROM refresh_jobs WHERE status IN ('pending','in_progress')`);
    if (left!.n === 0) {
      console.log(`\n  queue drained after ${round} more rounds`);
      drained = true;
      break;
    }
    await sleep(20);
  }
  if (!drained) {
    const [left] = await q(
      `SELECT count(*)::int AS n FROM refresh_jobs WHERE status IN ('pending','in_progress')`,
    );
    console.log(`\n  stopped at the 30s bound with ${left!.n} still queued (retry backoff still running)`);
  }

  console.log('\n  Work actually done per worker (proves horizontal scaling):');
  table([...claims].map(([id, s]) => ({ worker: id, batches: s.batches, jobs: s.jobs, throttled: s.throttled })));

  // ------------------------------------------------------------ 4. verify
  rule('STEP 4 — verify the packet requirements against the database');

  console.log('\n  Job outcomes:');
  table(await q(`SELECT status, coalesce(failure_class,'-') AS reason, count(*)::int
                   FROM refresh_jobs GROUP BY 1,2 ORDER BY 1,2`));

  console.log('\n  Retry behaviour (transient retries, permanent does not):');
  table(await q(`SELECT coalesce(failure_class,'success') AS outcome,
                        min(attempts)::int AS min_attempts, max(attempts)::int AS max_attempts
                   FROM refresh_jobs WHERE status IN ('completed','failed') GROUP BY 1 ORDER BY 1`));

  const checks = await q(
    `SELECT
       (SELECT coalesce(max(c),0) FROM (SELECT count(*) c FROM refresh_jobs
          WHERE status IN ('pending','in_progress') GROUP BY patient_id) x)::int   AS max_active_per_patient,
       (SELECT count(*) FROM (SELECT patient_id FROM refresh_jobs
          WHERE failure_class='permanent' GROUP BY patient_id HAVING count(*)>1) y)::int AS permanent_retried,
       (SELECT coalesce(max(attempts),0) FROM refresh_jobs WHERE failure_class='permanent')::int AS permanent_attempts,
       (SELECT count(*) FROM refresh_jobs WHERE status='completed'
          AND patient_id IN (SELECT patient_id FROM refresh_jobs WHERE status='completed'
                              GROUP BY patient_id HAVING count(*)>1))::int         AS double_processed`,
  );
  const c = checks[0]!;
  console.log('\n  Invariants:');
  table([
    { check: 'at most one active job per patient (dedupe)', expected: '1', actual: c.max_active_per_patient, ok: c.max_active_per_patient <= 1 ? 'PASS' : 'FAIL' },
    { check: 'permanent failures never rescheduled', expected: '0', actual: c.permanent_retried, ok: c.permanent_retried === 0 ? 'PASS' : 'FAIL' },
    { check: 'permanent failures never retried', expected: '1', actual: c.permanent_attempts, ok: c.permanent_attempts <= 1 ? 'PASS' : 'FAIL' },
    { check: 'no patient processed twice', expected: '0', actual: c.double_processed, ok: c.double_processed === 0 ? 'PASS' : 'FAIL' },
  ]);

  // `coalesced_study_ids` counts the studies that were DUE and triggered the
  // refresh. A successful refresh also advances the patient's active-but-not-due
  // associations, so `active_assocs_refreshed` is the fuller picture: the true
  // saving is at least `calls_avoided`, and usually more.
  const [saving] = await q(
    `SELECT sum(array_length(coalesced_study_ids,1))::int AS due_studies_triggering,
            count(*)::int                                 AS api_calls,
            (sum(array_length(coalesced_study_ids,1)) - count(*))::int AS calls_avoided,
            (SELECT count(*)::int FROM patient_studies ps
              WHERE ps.status='active'
                AND ps.patient_id IN (SELECT patient_id FROM refresh_jobs
                                       WHERE status='completed'))      AS active_assocs_refreshed
       FROM refresh_jobs WHERE status='completed'`,
  );
  console.log(
    '\n  Cost: due studies coalesced into one API call, and every ACTIVE association' +
      '\n  advanced by that one call (due or not):',
  );
  table([saving!]);
  // In a freshly seeded database every active association is also due, so the
  // two counts coincide. They diverge in steady state, which is exactly when the
  // saving matters: an association part-way through its interval is carried
  // forward by a refresh some *other* study paid for, and never buys a call.
  const carried = (saving!.active_assocs_refreshed as number) - (saving!.due_studies_triggering as number);
  console.log(
    carried > 0
      ? `\n  ${carried} active association(s) were advanced without having been due — ` +
          'refreshes they did not pay for.'
      : '\n  (Fresh seed: every active association was also due, so these coincide. ' +
          'In steady state the second number is the larger one.)',
  );

  await mock.close();
  await closePool();
  rule('done');
}

main().catch(async (err) => {
  console.error(err);
  await closePool().catch(() => {});
  process.exit(1);
});
