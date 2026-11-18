#!/bin/sh
# Stands in for the pipelines balancer: a queue bound to occurrence / occurrence.pipelines.balancer,
# so the orchestrator's pipelines requests are routable and can be inspected in the management UI.
set -eu
. "$(dirname "$0")/lib.sh"

curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' -X PUT \
  "$RABBIT_API/exchanges/%2F/occurrence" -d '{"type":"topic","durable":true}'
curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' -X PUT \
  "$RABBIT_API/queues/%2F/local.balancer" -d '{"durable":true}'
curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' -X POST \
  "$RABBIT_API/bindings/%2F/e/occurrence/q/local.balancer" -d '{"routing_key":"occurrence.pipelines.balancer"}'
echo "Queue local.balancer receives the pipelines requests"
