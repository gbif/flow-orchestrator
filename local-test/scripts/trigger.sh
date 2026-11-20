#!/bin/sh
# Starts a DwC-DP validation through the engine only, with the command flow-orchestrator-ws sends:
# trigger.sh UUID [ATTEMPT]. The analyser reads the data package from the shared store (see
# "task datapackage" in USAGE.md).
set -eu
. "$(dirname "$0")/lib.sh"

dataset="$1"
attempt="${2:-1}"

command=$(cat <<JSON
{"commandId":"$(uuid)","type":"START","domain":"VALIDATION","datasetKey":"$dataset","attempt":$attempt,
 "parameters":{"fileFormat":"DWCDP"},"requestedBy":"local-test","requestedAt":"$(now)"}
JSON
)
publish orchestrator validation.command "$command"
