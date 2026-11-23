#!/bin/sh
# Stands in for the pipelines workers' queues, so the orchestrator's and the ws's pipelines requests
# are routable and can be inspected in the management UI:
#   local.balancer           occurrence / occurrence.pipelines.balancer (the balancer)
#   local.archive.validator  occurrence / occurrence.pipelines.archive.validator.validator
#   local.checklist.validator occurrence / occurrence.pipelines.checklist.validator (from the ws)
set -eu
. "$(dirname "$0")/lib.sh"
require_rabbit_api

curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' -X PUT \
  "$RABBIT_API/exchanges/%2F/occurrence" -d '{"type":"topic","durable":true}'

bind() {
  curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' -X PUT \
    "$RABBIT_API/queues/%2F/$1" -d '{"durable":true}'
  curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' -X POST \
    "$RABBIT_API/bindings/%2F/e/occurrence/q/$1" -d "{\"routing_key\":\"$2\"}"
  echo "Queue $1 receives $2"
}

bind local.balancer occurrence.pipelines.balancer
bind local.archive.validator occurrence.pipelines.archive.validator.validator
bind local.checklist.validator occurrence.pipelines.checklist.validator
