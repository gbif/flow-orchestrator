-- Access for flow-orchestrator-ws. PostgreSQL only; applied by hand per environment after 003, as
-- the orchestrator role (the owner) or an admin. Role names are placeholders: confirm them first.
--
-- The ws writes only the validation tables; it reads workflow state and reports to compose a
-- validation's status. The engine (orchestrator, the owner) and registry (SELECT through the
-- default privileges of 001) need nothing here.

GRANT USAGE ON SCHEMA orchestrator TO orchestrator_ws;

GRANT SELECT, INSERT, UPDATE ON orchestrator.validation TO orchestrator_ws;
GRANT SELECT, INSERT, UPDATE ON orchestrator.shedlock TO orchestrator_ws;

GRANT SELECT ON orchestrator.workflow_execution, orchestrator.step_execution,
  orchestrator.step_attempt, orchestrator.workflow_report TO orchestrator_ws;
