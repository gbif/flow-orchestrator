#!/bin/sh
# Shared helpers: RabbitMQ management API and psql in the Compose environment.
set -eu

RABBIT_API="${RABBIT_API:-http://127.0.0.1:15672/api}"
RABBIT_AUTH="${RABBIT_AUTH:-localtest:localtest}"
COMPOSE="${COMPOSE:-podman compose -f compose.yaml}"

uuid() { cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen | tr 'A-Z' 'a-z'; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# Escapes a JSON document as a JSON string value.
json_string() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr -d '\n'; }

# publish EXCHANGE ROUTING_KEY JSON: publishes persistently and fails when the message is unroutable.
publish() {
  body=$(printf '{"properties":{"content_type":"application/json","delivery_mode":2},"routing_key":"%s","payload":"%s","payload_encoding":"string"}' \
    "$2" "$(json_string "$3")")
  result=$(curl -sf -u "$RABBIT_AUTH" -H 'content-type: application/json' \
    -X POST "$RABBIT_API/exchanges/%2F/$1/publish" -d "$body")
  case "$result" in
    *'"routed":true'*) echo "Published to $1 / $2" ;;
    *) echo "Not routed to $1 / $2 (is the orchestrator running?): $result" >&2; exit 1 ;;
  esac
}

# sql QUERY: runs a query in the registry database, unaligned output.
sql() { $COMPOSE exec -T postgres psql -U localtest -d registry -tA -F '|' -c "$1"; }
