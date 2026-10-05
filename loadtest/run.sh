#!/usr/bin/env bash
# Runs the load-test gate (LLD §19) on this machine: PostgreSQL, one api node, two engine
# nodes and the worker fleet, then k6 and the gate side by side. Output goes to loadtest/out/.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=loadtest/out
POOLS=${POOLS:-4}
BASE_RATE=${BASE_RATE:-500}
BURST_RATE=${BURST_RATE:-5000}
WARMUP=${WARMUP:-30}
BURST=${BURST:-60}
COOLDOWN=${COOLDOWN:-30}
WORKERS=${WORKERS:-40}
SLOTS=${SLOTS:-25}
WORK=${WORK:-20ms}
K6=${K6:-k6}
PG_PORT=${PG_PORT:-55600}
RUN=$(date +%s)

export JS_WORKER_TOKEN=load-test-worker-token JS_LOG_FORMAT=json JS_LOG_LEVEL=${JS_LOG_LEVEL:-warn} JS_DB_MAX_CONNS=${JS_DB_MAX_CONNS:-30}

rm -rf "$OUT" && mkdir -p "$OUT/bin"
pids=()
cleanup() {
  for pid in ${pids[@]+"${pids[@]}"}; do kill -INT "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ -n ${PGBIN:-} ]]; then "$PGBIN/pg_ctl" -D "$OUT/pg" stop -m fast >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

go build -o "$OUT/bin/jobscheduler" ./cmd/jobscheduler
go build -o "$OUT/bin/worker" ./loadtest/worker
go build -o "$OUT/bin/gate" ./loadtest/gate

initdb_path() {
  command -v initdb 2>/dev/null && return 0
  for dir in "${XDG_CACHE_HOME:-$HOME/.cache}"/jobscheduler/pgtest/*/bin "$HOME"/Library/Caches/jobscheduler/pgtest/*/bin; do
    if [[ -x $dir/initdb ]]; then echo "$dir/initdb"; return 0; fi
  done
  return 1
}

if [[ -z ${JS_DATABASE_URL:-} ]]; then
  INITDB=$(initdb_path) || { echo "no initdb found: run 'make test' once to cache PostgreSQL, or set JS_DATABASE_URL" >&2; exit 2; }
  PGBIN=$(dirname "$INITDB")
  "$PGBIN/initdb" -D "$OUT/pg" -U postgres --auth=trust >/dev/null
  # Durable commits stay on (fsync, synchronous_commit); only memory and checkpoints are tuned.
  "$PGBIN/pg_ctl" -D "$OUT/pg" -l "$OUT/postgres.log" -w \
    -o "-p $PG_PORT -k /tmp -c listen_addresses=localhost -c max_connections=200 -c shared_buffers=1GB -c max_wal_size=8GB -c checkpoint_timeout=30min" \
    start >/dev/null
  export JS_DATABASE_URL="postgres://postgres@localhost:$PG_PORT/postgres?sslmode=disable"
fi

JS=$OUT/bin/jobscheduler
"$JS" migrate >"$OUT/migrate.log" 2>&1

start() {
  local name=$1
  shift
  env "$@" "$JS" serve >"$OUT/$name.log" 2>&1 &
  pids+=($!)
}
start api JS_ROLES=api JS_NODE_ID=api JS_HTTP_ADDR=:18180 JS_OPS_ADDR=:19190 JS_DB_MAX_CONNS="${API_DB_MAX_CONNS:-$JS_DB_MAX_CONNS}"
start engine-a JS_ROLES=engine JS_NODE_ID=engine-a JS_HTTP_ADDR=:18181 JS_OPS_ADDR=:19191 \
  JS_WORKER_ADDR=:17170 JS_WORKER_ADVERTISE_ADDR=localhost:17170
start engine-b JS_ROLES=engine JS_NODE_ID=engine-b JS_HTTP_ADDR=:18182 JS_OPS_ADDR=:19192 \
  JS_WORKER_ADDR=:17171 JS_WORKER_ADVERTISE_ADDR=localhost:17171
for port in 19190 19191 19192; do
  for _ in $(seq 100); do curl -sf -o /dev/null "localhost:$port/readyz" && break; sleep 0.2; done
  curl -sf -o /dev/null "localhost:$port/readyz" || { echo "node on ops port $port is not ready; see $OUT/*.log" >&2; exit 2; }
done

API=http://localhost:18180
key() { sed -E 's/.*"api_key":"([^"]+)".*/\1/' "$1"; }
tenant() { sed -E 's/.*"tenant_id":"([^"]+)".*/\1/' "$1"; }
call() { # call METHOD PATH KEY BODY: fails unless the response is 2xx
  curl -sf -o /dev/null -X "$1" "$API$2" -H "Authorization: Bearer $3" -H 'Content-Type: application/json' -d "$4"
}
"$JS" bootstrap "load-admin-$RUN" platform-admin >"$OUT/admin.json"
keys=()
for n in $(seq 0 $((POOLS - 1))); do
  "$JS" bootstrap "load-$RUN-$n" >"$OUT/tenant-$n.json"
  call PUT "/v1/tenants/$(tenant "$OUT/tenant-$n.json")/quotas" "$(key "$OUT/admin.json")" '{"rate_limit": 1000000}'
  call POST /v1/job-types "$(key "$OUT/tenant-$n.json")" "{\"name\": \"load.noop\", \"pool\": \"load-$n\"}"
  keys+=("\"$(key "$OUT/tenant-$n.json")\"")
done
(IFS=,; echo "[${keys[*]}]") >"$OUT/keys.json"

"$OUT/bin/worker" -addr localhost:17170,localhost:17171 -pools "$POOLS" -workers "$WORKERS" -slots "$SLOTS" \
  -work "$WORK" >"$OUT/worker.log" 2>&1 &
pids+=($!)
owned() { curl -sf "$API/v1/pools" -H "Authorization: Bearer $(key "$OUT/admin.json")" | grep -o '"node_id"' | wc -l; }
for _ in $(seq 100); do [[ $(owned) -ge $POOLS ]] && break; sleep 0.2; done
[[ $(owned) -ge $POOLS ]] || { echo "pools have no owner; see $OUT/*.log" >&2; exit 2; }

echo "load test: $BASE_RATE/s for ${WARMUP}s, $BURST_RATE/s for ${BURST}s, $BASE_RATE/s for ${COOLDOWN}s; $POOLS pools; $WORKERS workers x $SLOTS slots"
# The gate measures the burst's steady part: from 5 s after the ramp up to 5 s before the ramp down.
# DB_CPU, when set, is a command printing the database's CPU seconds so far.
"$OUT/bin/gate" -metrics api=http://localhost:19190/metrics,engine-a=http://localhost:19191/metrics,engine-b=http://localhost:19192/metrics \
  -db-cpu "${DB_CPU:-}" -wait "$((WARMUP + 2 + 5))s" -window "$((BURST - 10))s" -rate "$BURST_RATE" >"$OUT/gate.txt" 2>&1 &
gate=$!
k6_status=0
"$K6" run --quiet -e API="$API" -e KEYS="$PWD/$OUT/keys.json" -e RUN="$RUN" -e BASE_RATE="$BASE_RATE" \
  -e BURST_RATE="$BURST_RATE" -e WARMUP="$WARMUP" -e BURST="$BURST" -e COOLDOWN="$COOLDOWN" \
  loadtest/submit.js 2>&1 | tee "$OUT/k6.txt" || k6_status=$?
gate_status=0
wait "$gate" || gate_status=$?
cat "$OUT/gate.txt"
if ((k6_status != 0 || gate_status != 0)); then
  echo "load test FAILED (k6 exit $k6_status, gate exit $gate_status); logs in $OUT/" >&2
  exit 1
fi
