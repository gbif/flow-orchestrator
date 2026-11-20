#!/bin/sh
# Port-forwards the local services to 127.0.0.1 until interrupted: ws 18081, engine 18080,
# Mailpit 18025 (web UI), RabbitMQ 5672 and 15672 (management), Postgres 15432. A forward stops
# when its pod restarts; run this again then.
set -eu
NAMESPACE="${NAMESPACE:-flow-orchestrator-local}"
KUBE_CONTEXT="${KUBE_CONTEXT:-minikube}"

pids=""
forward() {
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" port-forward "$@" >/dev/null &
  pids="$pids $!"
}
trap 'kill $pids 2>/dev/null' INT TERM EXIT

forward svc/flow-orchestrator 18080:8080
forward svc/rabbitmq 5672:5672 15672:15672
forward svc/postgres 15432:5432
forward svc/flow-orchestrator-ws 18081:8080
forward svc/mailpit 18025:8025
echo "Forwarding (ws 18081, engine 18080, Mailpit 18025, RabbitMQ 5672/15672, Postgres 15432); Ctrl-C stops"
wait
