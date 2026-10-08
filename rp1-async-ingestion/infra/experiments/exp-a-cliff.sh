#!/usr/bin/env bash
# Experiment (a) / Stage 4 gate: sync vs async under the same open-loop SFMC-style load and the same downstream
# capacity. Sweeps the arrival rate; per run records batch capture (first attempt / eventual), event capture after
# drain, ack latency, and server-side work amplification (sync).
#
# usage: exp-a-cliff.sh [--modes "sync async"] [--rates "40 60 ..."] [--duration 40] [--ens-suspension]
#   --ens-suspension: model SFMC ENS callback suspension (official docs: 2+ consecutive delivery failures in 5 min
#   -> suspended 10 min, events dropped meanwhile). Time-scaled 60x for a short run: 10 min -> 10 s.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -i "$0" "$@"; fi
source "$(dirname "$0")/lib.sh"

MODES="sync async"; RATES="40 60 70 80 90 100 120"; DURATION=40; ENS=()
BATCH=20; CAPACITY=8; SERVICE_TIME=5ms
while [[ $# -gt 0 ]]; do case $1 in
  --modes) MODES=$2; shift 2;; --rates) RATES=$2; shift 2;; --duration) DURATION=$2; shift 2;;
  --ens-suspension) ENS=(--suspend-after 2 --suspend-ms 10000); shift;;
  *) echo "unknown $1"; exit 2;; esac; done

require_jars
OUT="$ROOT/docs/results/stage5/a-cliff/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$OUT"
cat > "$OUT/README.md" <<EOF
# Experiment (a): sync vs async cliff
- Load: open loop, ${BATCH} events/batch, ${DURATION}s per rate, client timeout 2000ms, 3 retries at 1s/2s/4s.
- Downstream (both modes): capacity ${CAPACITY} concurrent calls x ${SERVICE_TIME} per event
  => theoretical max ~ ${CAPACITY} / (${SERVICE_TIME} x ${BATCH}) batches/s.
- Sync: ingest 'sync' profile does all work before responding. Async: ingest acks after acks=all produce;
  one consumer instance (3 threads) does the same work.
- ENS suspension model: ${ENS[*]:-off}.
- Rates: ${RATES}. Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(($(sysctl -n hw.memsize)/1073741824)) GB RAM; Docker VM $(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f GB", $1/1073741824}').
EOF

for mode in $MODES; do
  for rate in $RATES; do
    run="$OUT/$mode${ENS:+-ens}-$rate"; mkdir -p "$run.logs"
    log "=== mode=$mode rate=$rate/s ==="
    new_key; reset_env
    common=(--rp1.downstream.capacity=$CAPACITY --rp1.downstream.service-time=$SERVICE_TIME)
    if [[ $mode == sync ]]; then
      start_ingest sync "$run.logs" "${common[@]}"
    else
      start_ingest async "$run.logs"
      start_consumer 0 "$run.logs" "${common[@]}"
    fi
    loadgen_run "$run" --rate "$rate" --duration "$DURATION" --batch-size $BATCH "${ENS[@]}"
    log "sent: first=$(json "$run/summary.json" batchCaptureFirstAttempt) eventual=$(json "$run/summary.json" batchCaptureEventual)"
    loadgen_verify "$run" --stable-s 10 --max-wait-s 900
    curl -s --max-time 10 http://localhost:8080/actuator/prometheus > "$run/metrics-final.prom" || true
    log "verify: eventCapture=$(json "$run/verify.json" eventCapture) lost=$(json "$run/verify.json" lostUnaccounted) dup=$(json "$run/verify.json" duplicateRows)"
    stop_all
  done
done
python3 "$(dirname "$0")/summarize.py" "$OUT" > "$OUT/results.md"
cat "$OUT/results.md"
