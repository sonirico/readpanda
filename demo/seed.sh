#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(dirname "$0")/docker-compose.yml"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

max_tries=30
tries=0
until compose exec redpanda rpk cluster health >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge "$max_tries" ]; then
    echo "redpanda cluster did not become healthy after $max_tries tries" >&2
    exit 1
  fi
  sleep 2
done

topics=(
  "shop.orders.v1:6"
  "shop.orders.dlq:1"
  "shop.payments.v1:3"
  "shop.customers.v1:3"
  "iot.sensors.temperature:6"
  "iot.sensors.humidity:6"
  "iot.fleet.gps:3"
  "logs.api.access:3"
  "logs.api.error:1"
)

existing_topics="$(compose exec redpanda rpk topic list)"

for entry in "${topics[@]}"; do
  name="${entry%%:*}"
  partitions="${entry##*:}"
  if echo "$existing_topics" | grep -q "^${name}[[:space:]]"; then
    continue
  fi
  compose exec redpanda rpk topic create "$name" -p "$partitions"
done
