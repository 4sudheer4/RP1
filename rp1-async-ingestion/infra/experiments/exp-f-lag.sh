#!/usr/bin/env bash
# Experiment (f): arrival at ~1.5x consumer capacity (async). Ingest keeps acking fast; lag and the age of the
# oldest unprocessed message grow during the burst; after it, measure drain time.
# Capacity: one consumer, downstream 8 x 5ms/event => ~1600 events/s => 80 batches/s of 20. 1.5x = 120 b/s.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -i "$0" "$@"; fi
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-120}; DURATION=${DURATION:-60}

require_jars
OUT="$ROOT/docs/results/stage5/f-lag/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$OUT/logs"
new_key; reset_env
start_ingest async "$OUT/logs"
start_consumer 0 "$OUT/logs" --rp1.downstream.capacity=8 --rp1.downstream.service-time=5ms
echo "t_s,lag,oldest_age_s,processed_events" > "$OUT/lag.csv"
(
  t0=$(date +%s)
  while true; do
    m=$(curl -s --max-time 2 localhost:8081/actuator/prometheus)
    lag=$(echo "$m" | awk '/^rp1_consumer_lag\{/ {print $2}')
    age=$(echo "$m" | awk '/^rp1_consumer_oldest_unprocessed_age_seconds\{/ {print $2}')
    done_n=$(echo "$m" | awk '/^rp1_sink_events_total\{.*outcome="applied"/ {s+=$2} END {print s+0}')
    echo "$(( $(date +%s) - t0 )),${lag:-},${age:-},${done_n}" >> "$OUT/lag.csv"
    sleep 5
  done
) & SAMPLER=$!
PIDS+=($SAMPLER)
loadgen_run "$OUT/run" --rate "$RATE" --duration "$DURATION" --batch-size 20
burst_end=$(date +%s)
loadgen_verify "$OUT/run" --stable-s 15 --max-wait-s 900
drained=$(date +%s)
kill $SAMPLER 2>/dev/null || true
python3 - "$OUT" "$((drained - burst_end))" <<'EOF' > "$OUT/results.md"
import csv, json, sys
o, drain_s = sys.argv[1], sys.argv[2]; s = json.load(open(f"{o}/run/summary.json")); v = json.load(open(f"{o}/run/verify.json"))
rows = [r for r in csv.DictReader(open(f"{o}/lag.csv")) if r["lag"]]
peak_lag = max((float(r["lag"]) for r in rows), default=0); peak_age = max((float(r["oldest_age_s"] or 0) for r in rows), default=0)
print("# Experiment (f): 1.5x capacity burst -> lag build-up and drain (async)\n")
print(f"- arrival: {s['config']['rateBatchesPerSec']} batches/s x {s['config']['batchSize']} events for {s['config']['durationS']}s")
print(f"- ingest first-attempt batch capture during burst: {s['batchCaptureFirstAttempt']}, ack latency ms: {s['firstAttemptLatencyMs']}")
print(f"- peak consumer lag (records): {peak_lag:.0f}; peak oldest-unprocessed age: {peak_age:.1f}s (sampled every 5s, monitor samples every 10s)")
print(f"- drain time after burst end (until persisted count stable for 15s, includes the 15s): {drain_s}s")
for k in ["validEvents", "persistedDistinct", "lostUnaccounted", "duplicateRows", "statusMismatches", "eventCapture"]:
    print(f"- {k}: {v[k]}")
print("\nSamples: lag.csv")
EOF
cat "$OUT/results.md"
