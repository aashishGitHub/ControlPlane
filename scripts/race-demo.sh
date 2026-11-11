#!/usr/bin/env bash
# Race demo: prove the DB is the arbiter, not application locking.
#
#   1. 50 concurrent POSTs with the same slug -> exactly one 201, 49 x 409.
#   2. 50 concurrent PATCHes with the same version -> exactly one 202, 49 x 409.
#
# Requires the full stack (`make up`, worker included). Environment:
# BASE_URL (default http://localhost:8080), N (default 50).
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
N="${N:-50}"

command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 1; }

SLUG="race-$(date +%s)-${RANDOM}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# Count from per-request header dumps, not a shared status file: concurrent
# appends to one file are not reliable under MSYS/Git Bash on Windows.
echo "race 1: $N concurrent POSTs for slug '$SLUG'"
for i in $(seq 1 "$N"); do
  curl -sS -D "$TMP/post-headers.$i" -o /dev/null \
    -X POST "$BASE_URL/v1/tenants" \
    -H 'content-type: application/json' \
    -d "{\"slug\":\"$SLUG\",\"name\":\"Race Demo\"}" &
done
wait

created=0
conflicts=0
winner_header=""
for f in "$TMP"/post-headers.*; do
  first="$(head -n 1 "$f" | tr -d '\r')"
  case "$first" in
    *" 201 "*) created=$((created + 1)); winner_header="$f" ;;
    *" 409 "*) conflicts=$((conflicts + 1)) ;;
  esac
done
echo "  created=$created already_exists=$conflicts"
[[ "$created" -eq 1 ]] || fail "expected exactly one create winner"
[[ "$conflicts" -eq $((N - 1)) ]] || fail "expected $((N - 1)) conflicts"
[[ -n "$winner_header" ]] || fail "no 201 response captured"

# The winner's Location header is the tenant id. Listing tenants is not safe here:
# pagination starts at the oldest id, so a long-lived DB can push the new tenant
# off the first page.

tenant_id="$(awk 'tolower($1) == "location:" {print $2}' "$winner_header" | tr -d '\r' | sed 's#.*/##')"
[[ -n "$tenant_id" ]] || fail "created tenant id missing from Location header"

version=""
status=""
for _ in $(seq 1 80); do
  tenant_json="$(curl -fsS "$BASE_URL/v1/tenants/$tenant_id")"
  status="$(sed -n 's/.*"status":"\([^"]*\)".*/\1/p' <<< "$tenant_json")"
  version="$(sed -n 's/.*"version":\([0-9]*\).*/\1/p' <<< "$tenant_json")"
  [[ "$status" == "active" ]] && break
  sleep 0.25
done
[[ "$status" == "active" ]] || fail "tenant did not become active (last status: $status)"

echo "race 2: $N concurrent PATCHes at version $version"
for i in $(seq 1 "$N"); do
  curl -sS -D "$TMP/patch-headers.$i" -o /dev/null \
    -X PATCH "$BASE_URL/v1/tenants/$tenant_id" \
    -H 'content-type: application/json' \
    -d "{\"name\":\"Race Winner $i\",\"version\":$version}" &
done
wait

accepted=0
rejected=0
for f in "$TMP"/patch-headers.*; do
  first="$(head -n 1 "$f" | tr -d '\r')"
  case "$first" in
    *" 202 "*) accepted=$((accepted + 1)) ;;
    *" 409 "*) rejected=$((rejected + 1)) ;;
  esac
done
echo "  accepted=$accepted rejected=$rejected"
[[ "$accepted" -eq 1 ]] || fail "expected exactly one PATCH winner"
[[ "$rejected" -eq $((N - 1)) ]] || fail "expected $((N - 1)) rejected PATCHes"

echo "OK: exactly one winner per race (slug=$SLUG)"
