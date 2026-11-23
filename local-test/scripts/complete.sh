#!/bin/sh
# Simulates a worker's reply: complete.sh UUID STEP [OUTCOME] [ATTEMPT]
# OUTCOME is SUCCEEDED (default), FAILED, or INVALID (archive or identifier step: a rejecting
# report). The archive step answers as a checklist (with a ChecklistBank dataset) in the checklist
# and ColDP workflows.
set -eu
. "$(dirname "$0")/lib.sh"

dataset="$1"
step="$2"
outcome="${3:-SUCCEEDED}"
attempt="${4:-1}"

row=$(sql "SELECT e.id, e.workflow_name, s.current_attempt FROM workflow_execution e
           JOIN step_execution s ON s.workflow_execution_id = e.id
           WHERE e.domain = 'VALIDATION' AND e.dataset_key = '$dataset' AND e.attempt = $attempt
             AND s.step_type = '$step'
           ORDER BY e.run DESC LIMIT 1")
[ -n "$row" ] || { echo "No workflow with step $step for $dataset attempt $attempt" >&2; exit 1; }
workflow="${row%%|*}"
workflow_name=$(printf '%s' "$row" | cut -d'|' -f2)
step_attempt="${row##*|}"

outputs='{}'
reports='[]'
diagnostic='null'
valid=true; [ "$outcome" = INVALID ] && valid=false
case "$step" in
  VALIDATOR_VALIDATE_ARCHIVE)
    case "$workflow_name" in
      *checklist*|*coldp*) outputs='{"datasetType":"CHECKLIST","clbDatasetKey":"4711"}' ;;
      *) outputs='{"datasetType":"OCCURRENCE"}' ;;
    esac
    reason='null'; [ "$valid" = false ] && reason='"EML_NOT_FOUND: simulated"'
    reports="[{\"type\":\"ARCHIVE_VALIDATION\",\"content\":{\"valid\":$valid,\"invalidationReason\":$reason,\"files\":[{\"fileName\":\"occurrence.txt\",\"fileType\":\"CORE\",\"count\":10,\"rowType\":\"http://rs.tdwg.org/dwc/terms/Occurrence\"}]}}]" ;;
  VALIDATOR_CHECKLISTBANK)
    reports='[{"type":"CHECKLIST","content":{"clbDatasetKey":4711,"importStatus":"finished","indexeable":true,"files":[{"fileName":"NameUsage.tsv","count":3}]}}]'
    outputs='{"datasetType":"CHECKLIST"}' ;;
  VALIDATOR_DWCA_TO_VERBATIM)
    outputs='{"datasetType":"OCCURRENCE","occurrenceRecords":"10","eventRecords":"0","interpretTypes":"ALL","tripletValid":"true","occurrenceIdValid":"true"}' ;;
  VALIDATOR_DWCDP_TO_VERBATIM)
    outputs='{"datasetType":"OCCURRENCE","occurrenceRecords":"10","eventRecords":"0"}'
    reports='[{"type":"DWCDP_CONVERSION","content":{"statistics":{"simulated":true}}}]' ;;
  VALIDATOR_VERBATIM_TO_IDENTIFIER)
    reports="[{\"type\":\"IDENTIFIERS\",\"content\":{\"totalRecords\":10,\"absentIdentifierRecords\":0,\"duplicateIdentifiers\":0,\"isResultValid\":$valid,\"validationMessage\":\"simulated, valid=$valid\"}}]" ;;
  VALIDATOR_COLLECT_METRICS)
    reports='[{"type":"INTERPRETATION_METRICS","content":{"indexeable":true,"files":[{"rowType":"http://rs.tdwg.org/dwc/terms/Occurrence","indexedCount":10}]}}]' ;;
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
