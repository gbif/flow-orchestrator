#!/bin/sh
# Shows the workflows of a dataset: status.sh UUID
set -eu
. "$(dirname "$0")/lib.sh"

dataset="$1"
echo "== workflow_execution"
sql "SELECT attempt, run, workflow_name, status, rejected, coalesce(reason, '') FROM workflow_execution
     WHERE dataset_key = '$dataset' ORDER BY attempt, run"
echo "== step_execution"
sql "SELECT e.attempt, e.run, s.step_order, s.step_type, s.status, s.current_attempt, coalesce(s.reason, '')
     FROM step_execution s JOIN workflow_execution e ON e.id = s.workflow_execution_id
     WHERE e.dataset_key = '$dataset' ORDER BY e.attempt, e.run, s.step_order"
echo "== workflow_report"
sql "SELECT attempt, report_type, comment, length(report::text) FROM workflow_report
     WHERE dataset_key = '$dataset' ORDER BY attempt, report_type"
echo "== outbox_message (unpublished)"
sql "SELECT o.step_type, o.routing_key, o.publish_attempts, coalesce(o.last_error, '')
     FROM outbox_message o JOIN workflow_execution e ON e.id = o.workflow_execution_id
     WHERE e.dataset_key = '$dataset' AND o.published IS NULL"
