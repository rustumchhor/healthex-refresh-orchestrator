/** Drop, recreate and reseed. Destructive by design — `npm run db:reset`. */
import { resetDatabase } from '../src/migrate.js';
import { closePool, pool } from '../src/db.js';

await resetDatabase();

const { rows } = await pool.query(
  `SELECT (SELECT count(*) FROM patients)::int        AS patients,
          (SELECT count(*) FROM studies)::int         AS studies,
          (SELECT count(*) FROM patient_studies)::int AS enrollments,
          (SELECT count(*) FROM patient_endpoints)::int AS endpoint_links`,
);
console.log('database reset:', rows[0]);

await closePool();
