#!/bin/sh
# Shared helpers: RabbitMQ management API (through "task forward") and psql in the minikube
# environment.
set -eu

RABBIT_API="${RABBIT_API:-http://127.0.0.1:15672/api}"
RABBIT_AUTH="${RABBIT_AUTH:-localtest:localtest}"
NAMESPACE="${NAMESPACE:-flow-orchestrator-local}"
KUBE_CONTEXT="${KUBE_CONTEXT:-minikube}"

uuid() { cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen | tr 'A-Z' 'a-z'; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# Escapes a JSON document as a JSON string value.
json_string() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr -d '\n'; }

# require_rabbit_api: fails with a hint when the management API is not reachable (curl exit 7);
# it is only on 127.0.0.1 while "task forward" runs, and a forward dies when its pod restarts.
require_rabbit_api() {
  curl -sf -o /dev/null -u "$RABBIT_AUTH" "$RABBIT_API/overview" || {
    echo "RabbitMQ management API not reachable at $RABBIT_API: start (or restart) 'task forward'" >&2
    exit 1
  }
}

# publish EXCHANGE ROUTING_KEY JSON: publishes persistently and fails when the message is unroutable.
publish() {
  require_rabbit_api
  body=$(printf '{"properties":{"content_type":"application/json","delivery_mode":2},"routing_key":"%s","payload":"%s","payload_encoding":"string"}' \
    "$2" "$(json_string "$3")")
  result=$(curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' \
    -X POST "$RABBIT_API/exchanges/%2F/$1/publish" -d "$body")
  case "$result" in
    *'"routed":true'*) echo "Published to $1 / $2" ;;
    *) echo "Not routed to $1 / $2 (are the orchestrator and 'task forward' running?): $result" >&2; exit 1 ;;
  esac
}

# sql QUERY: runs a read-only query in the orchestrator schema, as registry does (registry role,
# SELECT only), unaligned output.
sql() {
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec -i deploy/postgres -- env PGOPTIONS='-c search_path=orchestrator' \
    psql -U registry -d registry -tA -F '|' -c "$1"
}
