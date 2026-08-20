-- Patient Data Refresh Orchestrator — schema
--
-- CORE OPERATION 0 — establish the durable data model and invariants.
-- WALKTHROUGH: Present this before Operation 1. Focus on patient-level jobs,
-- the one-active-job partial unique index, leases, the priority claim index,
-- and one rate-limit bucket per EHR endpoint. This is presentation setup, not
-- a separately numbered operation from the exercise.
--
-- Design notes that are load-bearing (see README for full reasoning):
--
--   * Postgres is both the queue and the system of record. There is no second
--     store, so job state, dedupe, retry budget and rate-limit tokens are all
--     mutated inside a single transaction.
--
--   * Every timestamp is written using the database's now(), never the app's
--     clock. With N workers on N hosts, app clock skew silently corrupts lease
--     expiry and retry backoff. One clock source is a correctness requirement,
--     not a stylistic preference.
--
--   * The unit of work is the PATIENT, because the API we were given is
--     patient-level: POST /patients/{id}/$updateData. Endpoints are not an
--     execution dimension — they are a rate-limiting dimension, since one
--     patient-level call fans out server-side to every endpoint holding that
--     patient's data. HealthEx confirmed this reading: one $updateData call
--     represents one patient-level refresh of all connected EHRs, and the
--     individual EHR rate limits must still be handled.

BEGIN;

DROP TABLE IF EXISTS refresh_jobs CASCADE;
DROP TABLE IF EXISTS rate_limit_buckets CASCADE;
DROP TABLE IF EXISTS patient_endpoints CASCADE;
DROP TABLE IF EXISTS patient_studies CASCADE;
DROP TABLE IF EXISTS ehr_endpoints CASCADE;
DROP TABLE IF EXISTS studies CASCADE;
DROP TABLE IF EXISTS patients CASCADE;

-- ---------------------------------------------------------------------------
-- Reference data. Deliberately minimal: only what the orchestrator needs to
-- decide *when* a patient is due and *which* rate limits their call consumes.
-- No clinical or FHIR resource modelling.
-- ---------------------------------------------------------------------------

CREATE TABLE patients (
  id           bigserial PRIMARY KEY,
  -- The {id} used in POST /patients/{id}/$updateData.
  external_ref text NOT NULL UNIQUE
);

CREATE TABLE studies (
  id               bigserial PRIMARY KEY,
  name             text     NOT NULL UNIQUE,
  -- "Patients enroll in studies with different refresh frequencies."
  refresh_interval interval NOT NULL,
  -- Higher wins. Claim order is priority DESC, so this is how a study says
  -- "my data is more urgent than the average enrollment".
  base_priority    int      NOT NULL DEFAULT 100
);

CREATE TABLE ehr_endpoints (
  id                 bigserial PRIMARY KEY,
  key                text NOT NULL UNIQUE,
  display_name       text NOT NULL,
  -- "EHR endpoints have varying rate limits (Epic ~100/min, others ~30/min)."
  rate_limit_per_min int  NOT NULL
);

-- Enrollment. This is what drives eligibility: refresh_interval measured from
-- last_refresh_at says whether the patient is due on this study's behalf.
CREATE TABLE patient_studies (
  patient_id         bigint      NOT NULL REFERENCES patients(id) ON DELETE CASCADE,
  study_id           bigint      NOT NULL REFERENCES studies(id)  ON DELETE CASCADE,
  status             text        NOT NULL DEFAULT 'active'
                                 CHECK (status IN ('active', 'withdrawn')),
  -- "Some refreshes are more urgent (consent expiring, user-triggered, etc)."
  consent_expires_at timestamptz,
  last_refresh_at    timestamptz,
  PRIMARY KEY (patient_id, study_id)
);

-- Which EHR endpoints hold data for this patient. Not an execution dimension:
-- the orchestrator issues one patient-level call. This exists so we can check
-- and consume the right rate-limit budgets before making that call.
CREATE TABLE patient_endpoints (
  patient_id  bigint NOT NULL REFERENCES patients(id)      ON DELETE CASCADE,
  endpoint_id bigint NOT NULL REFERENCES ehr_endpoints(id) ON DELETE CASCADE,
  PRIMARY KEY (patient_id, endpoint_id)
);

-- ---------------------------------------------------------------------------
-- The queue. One durable job per patient refresh.
-- ---------------------------------------------------------------------------

CREATE TABLE refresh_jobs (
  id                  bigserial   PRIMARY KEY,
  patient_id          bigint      NOT NULL REFERENCES patients(id) ON DELETE CASCADE,

  -- 3, AUDIT ONLY: which due enrollments caused this refresh, or were folded into
  -- it. A patient enrolled in a daily and a weekly study that both came due
  -- gets ONE refresh rather than two — the "avoid redundant work" requirement
  -- made concrete — and this column records that fact.
  --
  -- It is NOT the set of associations the refresh satisfies. On success the
  -- orchestrator advances last_refresh_at for every ACTIVE patient_studies row
  -- for the patient, due or not, because one patient-level call refreshes the
  -- patient's data for all of them. See src/domain/complete.ts.
  coalesced_study_ids bigint[]    NOT NULL,

  -- The four states named in the exercise. 'in_progress' covers both "a worker
  -- is holding this right now" and "the EHR is retrieving and we are polling";
  -- the two are distinguished by lease_expires_at and external_request_id.
  status              text        NOT NULL
                                  CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),

  priority            int         NOT NULL,

  -- The single scheduling knob. Carries initial jitter, retry backoff, 429
  -- cooldown and next-poll time. A job is invisible to claims until now().
  run_at              timestamptz NOT NULL,

  -- Hard stop. Bounds poll loops that never terminate, and poison jobs that
  -- kill a worker on every claim.
  deadline_at         timestamptz NOT NULL,

  -- Recorded $updateData attempts. NOT incremented by a worker crash and NOT
  -- incremented by a 429 — neither is the EHR rejecting our request on merit.
  attempts            int         NOT NULL DEFAULT 0,
  max_attempts        int         NOT NULL,

  -- Unanswered or unreadable sends. Tracked separately from `attempts` because
  -- they must not consume the retry budget (the idempotency key is derived from
  -- `attempts`), but they still need exponential backoff and a bound.
  ambiguous_attempts  int         NOT NULL DEFAULT 0,

  -- Lease. Held only while a worker is mid-HTTP-call, then released — including
  -- when the EHR is still retrieving. Workers never sit on in-flight work.
  locked_by           text,
  lease_expires_at    timestamptz,

  -- Set once $updateData is accepted. Its presence is what tells the next
  -- worker to poll GET .../data-retrieval/status instead of re-POSTing.
  external_request_id text,

  last_error          text,
  failure_class       text        CHECK (failure_class IN ('transient', 'permanent', 'rate_limited', 'timeout')),

  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  finished_at         timestamptz
);

-- 1
-- Duplicate prevention, enforced by the database rather than by a
-- check-then-insert race in application code. Two schedulers acting on the same
-- patient cannot both win: the loser's INSERT hits this index and is swallowed
-- by ON CONFLICT DO NOTHING.
--
-- This invariant — at most one active refresh per patient — is also precisely
-- why GET /patients/{id}/data-retrieval/status needs no request identifier.
-- Terminal rows are excluded, so history accumulates and the next cycle can
-- schedule the patient again freely.

-- 1 - partial unique) → duplicate prevention, race-proof by construction.
-- partial unique index covering only `pending` and `in_progress` rows. Historical completed/failed rows may accumulate,
-- but the database rejects a second live job for the same patient.
CREATE UNIQUE INDEX refresh_jobs_one_active_per_patient
  ON refresh_jobs (patient_id)
  WHERE status IN ('pending', 'in_progress');

-- 2 - Backs the claim query exactly: highest priority first, then oldest due.
-- Partial, so it indexes only live work and stays small regardless of how much
-- completed history the table accumulates.
CREATE INDEX refresh_jobs_claim
  ON refresh_jobs (priority DESC, run_at)
  WHERE status IN ('pending', 'in_progress');
  -- 4. run_at column carries jitter, retry backoff, 429 cooldown and next-poll time. 4 in 1.

-- Backs the "has this patient failed recently?" lookup that keeps the scheduler
-- from immediately re-creating a job that just failed permanently.
CREATE INDEX refresh_jobs_patient_terminal
  ON refresh_jobs (patient_id, finished_at DESC)
  WHERE finished_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Rate limiting.
--
-- One token bucket per EHR endpoint, refilled lazily on access. A patient-level
-- call consumes one token from EVERY endpoint holding that patient's data, so
-- acquisition is all-or-nothing across several buckets. Buckets are always
-- locked in ascending endpoint_id order to make deadlock impossible.
--
-- ASSUMPTION, not a confirmed fact: status polls are charged the same as
-- $updateData. Polls go through the ordinary claim path, so each one spends a
-- token from every one of the patient's endpoints. HealthEx confirmed that
-- per-endpoint limits must be respected but did NOT say whether polls share
-- that budget. Charging them is the conservative reading — the failure mode is
-- throttling ourselves harder than necessary, not exceeding a vendor limit.
--
-- Kept in its own table rather than as columns on ehr_endpoints because these
-- are the hottest rows in the system and the clearest scaling bottleneck —
-- worth being able to point at in isolation.
-- ---------------------------------------------------------------------------

CREATE TABLE rate_limit_buckets (
  endpoint_id    bigint           PRIMARY KEY REFERENCES ehr_endpoints(id) ON DELETE CASCADE,
  tokens         double precision NOT NULL,
  capacity       double precision NOT NULL,
  refill_per_sec double precision NOT NULL,
  last_refill_at timestamptz      NOT NULL DEFAULT now()
);

COMMIT;
