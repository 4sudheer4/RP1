#!/usr/bin/env bash
# Shared helpers for Stage 4/5 experiments. Source it; do not execute.
# Services run on the host as java -jar processes against the compose stack (infra/docker-compose.yml).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INFRA="$ROOT/infra"
INGEST_JAR="$ROOT/ingest-service/target/ingest-service-0.1.0-SNAPSHOT.jar"
CONSUMER_JAR="$ROOT/consumer-service/target/consumer-service-0.1.0-SNAPSHOT.jar"
LOADGEN_JAR="$ROOT/loadgen/target/loadgen-0.1.0-SNAPSHOT.jar"
JDBC_DIRECT="jdbc:postgresql://localhost:5442/rp1"
KAFKA_DIRECT="localhost:49092,localhost:49093,localhost:49094"
JAVA_OPTS_SVC="${JAVA_OPTS_SVC:--Xms256m -Xmx512m}"
PIDS=()

log() { echo "[$(date +%H:%M:%S)] $*"; }
dc()  { docker compose -f "$INFRA/docker-compose.yml" "$@"; }
tox() { curl -sf -X "$1" "http://localhost:8474$2" ${3:+-H 'Content-Type: application/json' -d "$3"} >/dev/null; }
psql_rp1() { docker exec -i rp1-postgres-1 psql -U rp1 -d rp1 -qAt "$@"; }

require_jars() {
  for j in "$INGEST_JAR" "$CONSUMER_JAR" "$LOADGEN_JAR"; do
    [[ -f $j ]] || { echo "missing $j: run 'mvn -q install -DskipTests' first" >&2; exit 1; }
  done
}

# Fresh random signing key per experiment: nothing secret is committed, and both sides agree.
new_key() {
  RP1_SIGNATURE_KEY="$(openssl rand -base64 32)"
  export RP1_SIGNATURE_KEY RP1_SIGNATURE_KEYS="$RP1_SIGNATURE_KEY"
}

ensure_brokers() {
  for b in kafka-1 kafka-2 kafka-3; do
    [[ $(docker inspect -f '{{.State.Running}}' "rp1-$b-1") == true ]] || docker start "rp1-$b-1" >/dev/null
  done
  local tries=0
  until [[ $(docker exec rp1-kafka-1-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 \
          --describe --under-replicated-partitions 2>/dev/null | wc -l | tr -d ' ') == 0 ]]; do
    ((tries++ > 90)) && { echo "cluster not fully in sync" >&2; exit 1; }
    sleep 1
  done
}

# Each run starts from an empty database, so the consumer group must start at the end of every topic; otherwise
# a new or lagging group replays history from earlier runs (auto.offset.reset=earliest) and contaminates the run.
reset_consumer_group() {
  local g=${CONSUMER_GROUP:-rp1-consumer} tries=0
  until docker exec rp1-kafka-1-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 \
          --group "$g" --reset-offsets --to-latest --all-topics --execute >/dev/null 2>&1; do
    ((tries++ > 30)) && { echo "could not reset group $g (members still active?)" >&2; exit 1; }
    sleep 1
  done
}

reset_env() {
  tox POST /reset
  ensure_brokers
  reset_consumer_group
  # Tables exist once a service has run Flyway; ignore errors on a brand-new database.
  psql_rp1 -c "TRUNCATE callback_event, message_status, invalid_event, downstream_call" 2>/dev/null || true
}

wait_http() {  # url timeout_s
  local url=$1 t=${2:-60} i=0
  until curl -sf "$url" 2>/dev/null | grep -q '"UP"'; do
    ((i++ > t)) && { echo "timeout waiting for $url" >&2; return 1; }
    sleep 1
  done
}

# start_ingest MODE LOGDIR [extra spring args...]
start_ingest() {
  local mode=$1 dir=$2; shift 2
  local profiles="textlogs"; [[ $mode == sync ]] && profiles="sync,textlogs"
  java $JAVA_OPTS_SVC -jar "$INGEST_JAR" --spring.profiles.active=$profiles --server.port=8080 "$@" \
    > "$dir/ingest.log" 2>&1 &
  PIDS+=($!); INGEST_PID=$!
  wait_http "http://localhost:8080/actuator/health/readiness" 90
  log "ingest up (mode=$mode pid=$INGEST_PID)"
}

# start_consumer INDEX LOGDIR [extra spring args...]  -> port 8081+INDEX
start_consumer() {
  local idx=$1 dir=$2; shift 2
  local port=$((8081 + idx))
  HOSTNAME="consumer-$idx" java $JAVA_OPTS_SVC -jar "$CONSUMER_JAR" --spring.profiles.active=textlogs \
    --server.port=$port "$@" > "$dir/consumer-$idx.log" 2>&1 &
  local pid=$!
  PIDS+=($pid); eval "CONSUMER_PID_$idx=$pid"
  wait_http "http://localhost:$port/actuator/health" 90
  log "consumer-$idx up (port=$port pid=$pid)"
}

stop_all() {
  for p in "${PIDS[@]:-}"; do [[ -n $p ]] && kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do [[ -n $p ]] && wait "$p" 2>/dev/null || true; done
  PIDS=()
}
trap stop_all EXIT

loadgen_run() {  # OUTDIR [loadgen run args...]
  local out=$1; shift
  java -Xmx1g -jar "$LOADGEN_JAR" run --url http://localhost:8080/callbacks --out "$out" \
    --metrics-url http://localhost:8080/actuator/prometheus "$@" > "$out.loadgen.log" 2>&1
}

loadgen_verify() {  # RUNDIR [extra]
  local run=$1; shift
  java -jar "$LOADGEN_JAR" verify --run "$run" --jdbc "$JDBC_DIRECT" --bootstrap "$KAFKA_DIRECT" "$@" \
    > "$run.verify.log" 2>&1
}

# json FILE KEY -> value (top-level or dotted path)
json() { python3 -c 'import json,sys;d=json.load(open(sys.argv[1]))
for k in sys.argv[2].split("."): d=d.get(k) if isinstance(d,dict) else None
print(d)' "$1" "$2"; }
