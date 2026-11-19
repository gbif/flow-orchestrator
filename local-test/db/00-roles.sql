-- Local roles standing in for the per-environment ones the db/ scripts name.
-- orchestrator owns and writes the orchestrator schema; registry may only read it.
CREATE ROLE orchestrator LOGIN PASSWORD 'localtest';
CREATE ROLE registry LOGIN PASSWORD 'localtest';
-- flow-orchestrator-ws: writes only the validation tables (db/004).
CREATE ROLE orchestrator_ws LOGIN PASSWORD 'localtest';
