#!/bin/sh
# Applies the table scripts as the orchestrator role, as in the real environments, so the
# orchestrator owns the tables and registry gets SELECT through the default privileges; then the
# grants for flow-orchestrator-ws.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c 'SET ROLE orchestrator' \
  -f /orchestrator-db/002-orchestrator-tables.sql \
  -f /orchestrator-db/003-validation-tables.sql \
  -f /orchestrator-db/004-validation-grants.sql
