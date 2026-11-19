-- The orchestrator schema, in the registry database. PostgreSQL only; applied by hand per
-- environment, as a role that may create schemas, BEFORE 002-orchestrator-tables.sql.
--
-- flow-orchestrator owns the schema and is the only writer; registry only reads. A later
-- orchestration-ws (or a move to its own database) starts from this boundary.
--
-- Role names are placeholders: confirm the orchestrator and registry roles per environment.

CREATE SCHEMA orchestrator AUTHORIZATION orchestrator;

-- Registry reads status and reports.
GRANT USAGE ON SCHEMA orchestrator TO registry;

-- Every table the orchestrator role creates in the schema, now or later, is readable by registry.
ALTER DEFAULT PRIVILEGES FOR ROLE orchestrator IN SCHEMA orchestrator
  GRANT SELECT ON TABLES TO registry;
