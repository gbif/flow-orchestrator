-- Validation tables (flow-orchestrator-ws), in the orchestrator schema.
--
-- Applied by hand per environment AS THE ORCHESTRATOR ROLE (so it owns them), after 002. Written
-- only by flow-orchestrator-ws (role grants in 004); the engine and registry read them.
-- Formerly validator-ws's own database (pipelines-validator-ws liquibase 001-004); its rows are
-- copied once per environment (see db/migration/validator-ws-data.md).
--
-- Portable SQL, run as is by the H2 tests: status and file_format are varchar (no Postgres enums,
-- so a new format needs no ALTER TYPE), and JSON is text, including notification_emails (a JSON
-- array; text[] in validator-ws). The key column keeps its name; H2 needs NON_KEYWORDS=KEY.

CREATE TABLE orchestrator.validation (
  key                    uuid                     NOT NULL PRIMARY KEY,
  source_id              text,
  installation_key       uuid,
  notification_emails    text,
  status                 varchar(32)              NOT NULL,
  file_format            varchar(32),
  username               varchar(255)             NOT NULL,
  file                   varchar(255)             NOT NULL,
  file_size              bigint,
  clb_dataset_key        integer,
  clb_validation_message text,
  dataset                text,
  metrics                text,
  created                timestamp with time zone NOT NULL,
  modified               timestamp with time zone NOT NULL,
  deleted                timestamp with time zone
);

-- The per-user running cap and the user's listing.
CREATE INDEX validation_username_status_idx ON orchestrator.validation (username, status);
-- Listing by date, retention, and the ChecklistBank poller.
CREATE INDEX validation_created_idx ON orchestrator.validation (created);

-- ShedLock: each scheduled job of flow-orchestrator-ws runs on one instance at a time. Do not delete
-- its rows while the service runs: ShedLock then skips the job until restarted.
CREATE TABLE orchestrator.shedlock (
  name       varchar(64)  NOT NULL PRIMARY KEY,
  lock_until timestamp    NOT NULL,
  locked_at  timestamp    NOT NULL,
  locked_by  varchar(255) NOT NULL
);

-- If an admin ran this script instead of the orchestrator role (PostgreSQL only):
--
-- ALTER TABLE orchestrator.validation OWNER TO orchestrator;
-- ALTER TABLE orchestrator.shedlock   OWNER TO orchestrator;
-- GRANT SELECT ON orchestrator.validation, orchestrator.shedlock TO registry;
