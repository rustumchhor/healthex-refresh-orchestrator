# Patient Data Refresh Orchestrator

## Introduction

The Patient Data Refresh Orchestrator schedules, executes, and tracks health-record refreshes for patients enrolled in studies. Study configuration determines when a patient is due, while each connected EHR endpoint contributes its own rate limit. The implementation includes a working PostgreSQL-backed queue, scheduler, horizontally scalable workers, retry handling, and a mock EHR API; only the external EHR service is simulated.

The orchestrator prevents duplicate work, prioritizes urgent refreshes, spreads scheduled load, enforces per-endpoint limits, and coalesces multiple due study enrollments into one patient-level API call.

## Architecture Design

```text
                 +-------------+
                 |  Admin API  |
                 +------+------+ 
                        |
+-----------+     +-----v------+     +-------------+     +----------+
| Scheduler | --> | PostgreSQL | <-- | Worker(s)   | --> | Mock EHR |
+-----------+     | queue and  |     | claim, call,|     | API      |
                  | state store|     | record      |     +----------+
                  +------------+     +-------------+
```

- The scheduler finds due patients, calculates priority, applies deterministic jitter, and creates jobs.
- PostgreSQL is both the durable system of record and the job queue. Transactions coordinate job claims, deduplication, leases, retries, and rate-limit tokens.
- Workers atomically claim jobs in priority order with `FOR UPDATE SKIP LOCKED`, call or poll the EHR API, and record the outcome.
- The mock EHR reproduces accepted, transient-failure, permanent-failure, and rate-limited responses.
- The admin API exposes user-triggered refreshes and operational status.

The main design tradeoff is using PostgreSQL instead of a separate queue. This keeps scheduling state and concurrency controls transactional and easy to inspect, but the shared rate-limit rows become a contention point at much larger scale. Patient-level jobs were chosen because the supplied API refreshes a patient across connected EHRs in one call; endpoint associations are therefore used for admission control rather than as separate jobs. With more time, the first improvements would be distributed quota leases, priority aging, adaptive polling, and stronger observability. Further decisions, tradeoffs, and future improvements are documented in [QuestionsAndClarifications.md](QuestionsAndClarifications.md).

## Schema Design

The schema contains seven tables and intentionally excludes clinical/FHIR resource modeling:

```text
patients ---- patient_studies ---- studies
    |
    +-------- patient_endpoints -- ehr_endpoints -- rate_limit_buckets
    |
    +-------- refresh_jobs
```

| Table | Purpose |
|---|---|
| `patients` | Identifies the patient used by the refresh API. |
| `studies` | Defines refresh frequency and base priority. |
| `patient_studies` | Tracks enrollment status, consent expiry, and last successful refresh. |
| `ehr_endpoints` | Defines connected EHR systems and their request limits. |
| `patient_endpoints` | Maps each patient to the endpoint budgets their refresh consumes. |
| `refresh_jobs` | Stores queue state, priority, scheduling, leases, attempts, failures, and external request IDs. |
| `rate_limit_buckets` | Stores the token-bucket state for each endpoint. |

Two partial indexes enforce the queue's core guarantees: one permits at most one active job per patient, and the other supports priority-ordered claims over live jobs. A successful patient refresh advances every active study association, while `coalesced_study_ids` records which due studies triggered the call. The complete definition is in [`db/schema.sql`](db/schema.sql).

## How to Run the Demo

Prerequisites: Docker with Docker Compose and Node.js 22 or later.

```bash
docker compose up -d postgres
npm install
npm run demo
```

The demo resets and seeds the local `healthex` database, so do not point `DATABASE_URL` at a database containing data you need to keep. It then runs the real scheduler and four workers against the mock EHR and prints database-backed evidence for scheduling, prioritization, deduplication, rate limiting, horizontal processing, retries, permanent failures, and avoided duplicate calls.

To stop the demo database afterward:

```bash
docker compose down
```
