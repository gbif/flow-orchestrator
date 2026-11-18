#!/bin/sh
# Simulates a pipelines step's reply: complete.sh UUID STEP [OUTCOME] [ATTEMPT]
# OUTCOME is SUCCEEDED (default), FAILED, or INVALID (identifier step: a rejecting report).
set -eu
. "$(dirname "$0")/lib.sh"

dataset="$1"
step="$2"
outcome="${3:-SUCCEEDED}"
attempt="${4:-1}"

row=$(sql "SELECT e.id, s.current_attempt FROM workflow_execution e
           JOIN step_execution s ON s.workflow_execution_id = e.id
           WHERE e.domain = 'VALIDATION' AND e.dataset_key = '$dataset' AND e.attempt = $attempt
             AND s.step_type = '$step'
           ORDER BY e.run DESC LIMIT 1")
[ -n "$row" ] || { echo "No workflow with step $step for $dataset attempt $attempt" >&2; exit 1; }
workflow="${row%%|*}"
step_attempt="${row##*|}"

outputs='{}'
reports='[]'
diagnostic='null'
case "$step" in
  VALIDATOR_DWCDP_TO_VERBATIM)
    outputs='{"datasetType":"OCCURRENCE","occurrenceRecords":"10","eventRecords":"0"}'
    reports='[{"type":"DWCDP_CONVERSION","content":{"statistics":{"simulated":true}}}]' ;;
  VALIDATOR_VERBATIM_TO_IDENTIFIER)
    valid=true; [ "$outcome" = INVALID ] && valid=false
    reports="[{\"type\":\"IDENTIFIERS\",\"content\":{\"totalRecords\":10,\"absentIdentifierRecords\":0,\"duplicateIdentifiers\":0,\"isResultValid\":$valid,\"validationMessage\":\"simulated, valid=$valid\"}}]" ;;
  VALIDATOR_COLLECT_METRICS)
    reports='[{"type":"INTERPRETATION_METRICS","content":{"indexeable":true,"fileInfos":[]}}]' ;;
esac
[ "$outcome" = INVALID ] && outcome=SUCCEEDED
if [ "$outcome" = FAILED ]; then
  diagnostic='"simulated failure"'; outputs='{}'; reports='[]'
fi

completed=$(cat <<JSON
{"messageId":"$(uuid)",
 "header":{"workflowId":"$workflow","datasetKey":"$dataset","attempt":$attempt,"stepType":"$step","stepAttempt":$step_attempt},
 "outcome":"$outcome","diagnostic":$diagnostic,"outputs":$outputs,"reports":$reports,
 "startedAt":"$(now)","finishedAt":"$(now)"}
JSON
)
publish orchestrator validation.step.completed "$completed"
