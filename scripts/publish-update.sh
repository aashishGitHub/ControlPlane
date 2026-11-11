#!/usr/bin/env bash
# Publish a fault-injection task update straight to RabbitMQ's HTTP API.
#
#   scripts/publish-update.sh duplicate <task_id> <tenant_id>
#   scripts/publish-update.sh stale     <task_id> <tenant_id>
#   scripts/publish-update.sh poison    <task_id> <tenant_id>
#
# Environment: RABBIT_URL (default http://localhost:15672), RABBIT_USER,
# RABBIT_PASS. TASK_ID/TENANT_ID may replace the positional args.
set -euo pipefail

MODE="${1:-}"
TASK_ID="${TASK_ID:-${2:-}}"
TENANT_ID="${TENANT_ID:-${3:-}}"
RABBIT_URL="${RABBIT_URL:-http://localhost:15672}"
RABBIT_USER="${RABBIT_USER:-guest}"
RABBIT_PASS="${RABBIT_PASS:-guest}"

usage() {
  cat <<'EOF'
usage: publish-update.sh <duplicate|stale|poison> <task_id> <tenant_id>

  duplicate  publish the same terminal "done" update twice (same message_id)
  stale      publish an in_progress update that is stale after done
  poison     publish an invalid JSON payload; it must land in the DLQ
EOF
  exit 2
}

case "$MODE" in
  duplicate|stale|poison) ;;
  *) usage ;;
esac
[[ -n "$TASK_ID" && -n "$TENANT_ID" ]] || usage

command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 1; }

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
  elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
  else
    powershell.exe -NoProfile -Command '[guid]::NewGuid().ToString()' | tr -d '\r'
  fi
}

publish() {
  local routing_key="$1" payload="$2" escaped body
  # The management API takes the payload as a JSON *string*, so quotes and
  # backslashes inside it must be escaped. sed keeps the script dependency-free.
  escaped="$(printf '%s' "$payload" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  body="$(printf '{"properties":{"delivery_mode":2},"routing_key":"%s","payload":"%s","payload_encoding":"string"}' \
    "$routing_key" "$escaped")"
  curl -fsS -u "$RABBIT_USER:$RABBIT_PASS" \
    -H 'content-type: application/json' \
    -X POST "$RABBIT_URL/api/exchanges/%2F/cp.task-updates/publish" \
    -d "$body" >/dev/null
  echo "published routing_key=$routing_key"
  echo "  payload=$payload"
}

valid_payload() {
  local message_id="$1" status="$2"
  printf '{"schema_version":1,"message_id":"%s","task_id":"%s","tenant_id":"%s","status":"%s","occurred_at":"%s"}' \
    "$message_id" "$TASK_ID" "$TENANT_ID" "$status" "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
}

case "$MODE" in
  duplicate)
    message_id="$(new_uuid)"
    payload="$(valid_payload "$message_id" done)"
    publish "task.update.done" "$payload"
    publish "task.update.done" "$payload"
    ;;
  stale)
    payload="$(valid_payload "$(new_uuid)" in_progress)"
    publish "task.update.in_progress" "$payload"
    ;;
  poison)
    publish "task.update.done" '{"schema_version":1,'
    ;;
esac
