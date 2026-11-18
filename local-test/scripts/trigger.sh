#!/bin/sh
# Starts a DwC-DP validation: trigger.sh UUID [ATTEMPT] [CHAIN]
# CHAIN is "analysis" (only the analyser) or "full" (the whole chain; see USAGE.md).
set -eu
. "$(dirname "$0")/lib.sh"

dataset="$1"
attempt="${2:-1}"
chain="${3:-analysis}"

command=$(cat <<JSON
{"commandId":"$(uuid)","type":"START","domain":"VALIDATION","datasetKey":"$dataset","attempt":$attempt,
 "parameters":{"fileFormat":"DWCDP","chain":"$chain"},"requestedBy":"local-test","requestedAt":"$(now)"}
JSON
)
publish orchestrator validation.command "$command"
