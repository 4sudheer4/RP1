#!/usr/bin/env bash
# Stage-1 gate: topic durability config + acks=all behavior under broker loss, through Toxiproxy.
#
#   infra/scripts/stage1-gate.sh            # uses running stack (starts it if needed)
#   infra/scripts/stage1-gate.sh --fresh    # down -v first (clean cluster)
#
# Output: docs/results/stage1/<run>/gate.log (ledgers alongside, gitignored). Exit 0 only if every check passes.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
INFRA=$ROOT/infra
RUN=$(date +%Y%m%dT%H%M%S)
OUT=$ROOT/docs/results/stage1/$RUN
LEDGER=$OUT/ledger/stage1-gate
LEDGER_MINISR=$OUT/ledger/minisr3
mkdir -p "$LEDGER" "$LEDGER_MINISR"
exec > >(tee "$OUT/gate.log") 2>&1

dc()  { docker compose -f "$INFRA/docker-compose.yml" "$@"; }
kt()  { dc exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 "$@"; }
kcfg(){ dc exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:29092 "$@"; }
tox() { curl -sf -X "$1" "http://localhost:8474$2" ${3:+-H 'Content-Type: application/json' -d "$3"} >/dev/null; }
gate(){ java -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
             -cp "$INFRA/gate/target/classes:$(cat "$INFRA/gate/target/cp.txt")" com.rp1.gate.Stage1Gate "$@"; }

PASS=0; FAIL=0; RESULTS=()
check() {  # name, then command; records PASS/FAIL
  local name=$1; shift
  if "$@"; then PASS=$((PASS+1)); RESULTS+=("PASS  $name"); echo "  -> PASS: $name"
  else FAIL=$((FAIL+1)); RESULTS+=("FAIL  $name"); echo "  -> FAIL: $name"; fi
}
step() { echo; echo "=== $(date +%H:%M:%S) $* ==="; }
now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time*1000'; }

# ------------------------------------------------------------------ setup
step "setup (run=$RUN)"
[[ "${1:-}" == "--fresh" ]] && dc down -v --remove-orphans
dc up -d --wait || { echo "stack failed to start"; exit 1; }
tox POST /reset   # clear leftover toxics, re-enable all proxies
(cd "$INFRA/gate" && mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt) || exit 1
docker --version; dc version --short; dc ps --format '{{.Service}}\t{{.Image}}\t{{.Status}}'

# ------------------------------------------------------------------ 1. config
step "1. topic + broker durability config"
assert_topic() {  # topic partitions min_isr
  local d; d=$(kt --describe --topic "$1") || return 1
  echo "$d" | head -1
  grep -q "PartitionCount: $2"$'\t' <<<"$d" &&
  grep -q "ReplicationFactor: 3" <<<"$d" &&
  grep -q "min.insync.replicas=$3" <<<"$d" &&
  grep -q "unclean.leader.election.enable=false" <<<"$d" &&
  # every partition fully replicated and in sync right now
  [[ $(grep -c $'\tPartition: ' <<<"$d") -eq $2 ]] &&
  ! grep $'\tPartition: ' <<<"$d" | grep -vqE 'Replicas: [123],[123],[123]'$'\t''Isr: [123],[123],[123]'
}
check "callback-events      12p RF3 minISR2 unclean=false, all ISR=3" assert_topic callback-events 12 2
check "callback-events-dlt   3p RF3 minISR2 unclean=false, all ISR=3" assert_topic callback-events-dlt 3 2
check "invalid-events        3p RF3 minISR2 unclean=false, all ISR=3" assert_topic invalid-events 3 2
check "stage1-gate          12p RF3 minISR2 unclean=false, all ISR=3" assert_topic stage1-gate 12 2

assert_broker_defaults() {
  local all=0
  for b in 1 2 3; do
    local c; c=$(kcfg --describe --entity-type brokers --entity-name $b --all)
    for kv in min.insync.replicas=2 default.replication.factor=3 unclean.leader.election.enable=false \
              auto.create.topics.enable=false offsets.topic.replication.factor=3; do
      grep -q " $kv " <<<"$c" || { echo "broker $b missing $kv"; all=1; }
    done
  done
  echo "brokers 1-3: min.insync.replicas=2 default.replication.factor=3 unclean=false auto.create=false offsets RF=3"
  return $all
}
check "broker defaults on all 3 brokers" assert_broker_defaults

# ------------------------------------------------------------------ 2. baseline
step "2. baseline: 1000 x acks=all via Toxiproxy, all brokers up"
check "baseline all acked" gate produce --topic stage1-gate --run "$RUN" --phase A-baseline --count 1000 --ledger "$LEDGER"

# ------------------------------------------------------------------ 3. toxiproxy in path
step "3. prove client traffic flows through Toxiproxy"
for b in 1 2 3; do tox POST /proxies/kafka-$b/toxics '{"name":"lat","type":"latency","stream":"downstream","attributes":{"latency":300}}'; done
latency_applied() {
  local o; o=$(gate produce --topic stage1-gate --run "$RUN" --phase B-latency300 --count 50 --ledger "$LEDGER") || { echo "$o"; return 1; }
  echo "$o"
  local p50; p50=$(sed -nE 's/.*p50=([0-9]+)\..*/\1/p' <<<"$o")
  echo "p50=${p50}ms (expect >= 300ms injected)"; [[ ${p50:-0} -ge 300 ]]
}
check "300ms latency toxic shows up in ack latency" latency_applied
for b in 1 2 3; do tox DELETE /proxies/kafka-$b/toxics/lat; done

for b in 1 2 3; do tox POST /proxies/kafka-$b '{"enabled":false}'; done
check "all proxies disabled -> every send fails (no bypass path)" \
  gate produce --topic stage1-gate --run "$RUN" --phase C-proxies-down --count 5 --expect all-failed \
       --max-block-ms 3000 --request-timeout-ms 2000 --delivery-timeout-ms 5000 --ledger "$LEDGER"
for b in 1 2 3; do tox POST /proxies/kafka-$b '{"enabled":true}'; done

# ------------------------------------------------------------------ 4. kill one broker mid-run
step "4. SIGKILL kafka-2 while producing 6000 msgs at 600/s"
gate produce --topic stage1-gate --run "$RUN" --phase D-kill-mid-run --count 6000 --rate 600 --ledger "$LEDGER" > "$OUT/phase-D.out" 2>&1 &
PID=$!
sleep 3
echo "$(date +%H:%M:%S) kill-epoch-ms=$(now_ms) docker kill -s SIGKILL kafka-2"; dc kill -s SIGKILL kafka-2
wait $PID; RC=$?
cat "$OUT/phase-D.out"
check "broker killed mid-run: every acks=all send still acked" test $RC -eq 0

step "4b. cluster state with kafka-2 down"
sleep 2
kt --describe --topic stage1-gate | sed -n '2,13p'
no_leader_on_dead_broker() {
  local d; d=$(kt --describe --topic stage1-gate | grep $'\tPartition: ')
  ! grep -qE 'Leader: (2|none|-1)'$'\t' <<<"$d" && ! grep -qE 'Isr: [^\t]*2' <<<"$d"
}
check "kafka-2 fenced: no partition led by it, removed from every ISR" no_leader_on_dead_broker

step "5. steady state with 1 of 3 brokers down: 1000 x acks=all"
check "degraded cluster (2/3) still accepts acks=all" \
  gate produce --topic stage1-gate --run "$RUN" --phase E-degraded --count 1000 --ledger "$LEDGER"

# ------------------------------------------------------------------ 6. min ISR enforcement
step "6. min ISR is enforced: topic with min.insync.replicas=3, one broker down"
minisr_out=$(gate produce --topic stage1-gate-minisr3 --run "$RUN" --phase F-minisr3 --count 20 --expect all-failed \
     --no-idempotence --retries 0 --request-timeout-ms 3000 --delivery-timeout-ms 5000 --ledger "$LEDGER_MINISR"); minisr_rc=$?
echo "$minisr_out"
check "acks=all rejected with NotEnoughReplicas when ISR < min ISR" \
  bash -c "[[ $minisr_rc -eq 0 ]] && grep -q NotEnoughReplicas <<<'$minisr_out'"

# ------------------------------------------------------------------ 7. two brokers down
step "7. warm producer running, SIGKILL kafka-3 too (1 of 3 alive, KRaft quorum lost): no ack after the kill"
gate produce --topic stage1-gate --run "$RUN" --phase G-two-down-warm --count 2000 --rate 100 --expect any \
     --max-block-ms 5000 --request-timeout-ms 3000 --delivery-timeout-ms 10000 --deadline-ms 60000 \
     --ledger "$LEDGER" > "$OUT/phase-G.out" 2>&1 &
PID=$!
sleep 3
KILL_MS=$(now_ms); echo "$(date +%H:%M:%S) kill-epoch-ms=$KILL_MS docker kill -s SIGKILL kafka-3"; dc kill -s SIGKILL kafka-3
wait $PID; RC=$?
cat "$OUT/phase-G.out"
no_ack_after_kill() {
  local last; last=$(sed -nE 's/.*last-ack-epoch-ms=([0-9]+).*/\1/p' "$OUT/phase-G.out")
  local failed; failed=$(sed -nE 's/.*acked=[0-9]+ failed=([0-9]+).*/\1/p' "$OUT/phase-G.out")
  echo "last ack $(( ${last:-0} - KILL_MS ))ms relative to kill (allow <= +1000ms for in-flight responses), failed after kill=$failed"
  [[ $RC -eq 0 && ${last:-0} -le $((KILL_MS + 1000)) && ${failed:-0} -gt 0 ]]
}
check "only 1 replica alive -> nothing acked after the kill, sends fail within delivery.timeout" no_ack_after_kill

step "7b. cold idempotent producer while quorum is lost (bounded by app-side deadline)"
check "cold producer: no write acked (app deadline 15s enforces the bound)" \
  gate produce --topic stage1-gate --run "$RUN" --phase H-two-down-cold --count 20 --expect all-failed \
       --max-block-ms 5000 --request-timeout-ms 3000 --delivery-timeout-ms 10000 --deadline-ms 15000 --ledger "$LEDGER"

# ------------------------------------------------------------------ 8. recovery + verify
step "8. restart kafka-2 and kafka-3, wait for full ISR"
dc start kafka-2 kafka-3
dc up -d --wait kafka-2 kafka-3 >/dev/null 2>&1
full_isr() {
  for i in $(seq 1 60); do
    if [[ -z $(kt --describe --under-replicated-partitions --topic stage1-gate) ]]; then echo "full ISR after ~$((i*2))s"; return 0; fi
    sleep 2
  done; return 1
}
check "all stage1-gate partitions back to ISR=3" full_isr

step "9. verify log against ledgers"
check "zero loss: every acked id is in the log" gate verify --topic stage1-gate --run "$RUN" --ledger "$LEDGER"

# ------------------------------------------------------------------ summary
step "summary"
printf '%s\n' "${RESULTS[@]}"
echo "passed=$PASS failed=$FAIL  log=$OUT/gate.log"
[[ $FAIL -eq 0 ]]
