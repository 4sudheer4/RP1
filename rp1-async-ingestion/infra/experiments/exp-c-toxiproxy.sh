#!/usr/bin/env bash
# Experiment (c): degrade the ingest<->Kafka link with Toxiproxy (async mode).
#   phase 1: 2500ms latency on all broker proxies (slower than the 1.5s deadline)
#   phase 2: link cut (proxies disabled)
# Expect: every response inside the deadline (fast 503s, no hang), breaker opens, full recovery afterwards,
# and SFMC-style retries bring eventual capture back up; zero lost acked events.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -i "$0" "$@"; fi
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-40}; DURATION=${DURATION:-100}

require_jars
OUT="$ROOT/docs/results/stage5/c-toxiproxy/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$OUT/logs"
new_key; reset_env
start_ingest async "$OUT/logs"
start_consumer 0 "$OUT/logs"
loadgen_run "$OUT/run" --rate "$RATE" --duration "$DURATION" --batch-size 20 &
LG=$!
sample() { curl -s --max-time 2 localhost:8080/actuator/prometheus | grep -E '^rp1_ingest_(breaker_state|inflight)' | tr '\n' ' '; }
sleep 20
log "phase 1: latency 2500ms"; echo "latency_on=$(date -u +%FT%TZ)" > "$OUT/events.txt"
for b in kafka-1 kafka-2 kafka-3; do tox POST "/proxies/$b/toxics" '{"name":"slow","type":"latency","stream":"downstream","attributes":{"latency":2500}}'; done
for i in 1 2 3 4; do sleep 5; echo "t+$((i*5))s $(sample)" >> "$OUT/samples.txt"; done
for b in kafka-1 kafka-2 kafka-3; do tox DELETE "/proxies/$b/toxics/slow"; done
echo "latency_off=$(date -u +%FT%TZ)" >> "$OUT/events.txt"
sleep 15
log "phase 2: cut"; echo "cut_on=$(date -u +%FT%TZ)" >> "$OUT/events.txt"
for b in kafka-1 kafka-2 kafka-3; do tox POST "/proxies/$b" '{"enabled":false}'; done
for i in 1 2 3; do sleep 5; echo "cut t+$((i*5))s $(sample)" >> "$OUT/samples.txt"; done
for b in kafka-1 kafka-2 kafka-3; do tox POST "/proxies/$b" '{"enabled":true}'; done
echo "cut_off=$(date -u +%FT%TZ)" >> "$OUT/events.txt"
wait $LG
loadgen_verify "$OUT/run" --stable-s 15 --max-wait-s 600
grep -E 'callback batch' "$OUT/logs/ingest.log" | sed -E 's/.*latencyMs=([0-9]+).*/\1/' | sort -n | tail -1 > "$OUT/server-max-latency-ms.txt"
python3 - "$OUT" <<'EOF' > "$OUT/results.md"
import json, sys
o = sys.argv[1]; s = json.load(open(f"{o}/run/summary.json")); v = json.load(open(f"{o}/run/verify.json"))
print("# Experiment (c): Toxiproxy latency then cut on the ingest->Kafka link (async)\n")
print(open(f"{o}/events.txt").read())
print("Server-side max handler latency (ms):", open(f"{o}/server-max-latency-ms.txt").read().strip(), "\n")
print("Samples (breaker_state 0=closed 1=open 2=half-open; inflight):\n```\n" + open(f"{o}/samples.txt").read() + "```")
for k in ["batches", "batchCaptureFirstAttempt", "batchCaptureEventual", "batchesDropped", "statusCounts", "firstAttemptLatencyMs", "allAttemptsLatencyMs"]:
    print(f"- {k}: {s[k]}")
for k in ["validEvents", "persistedDistinct", "missing", "lostUnaccounted", "duplicateRows", "statusMismatches", "eventCapture"]:
    print(f"- {k}: {v[k]}")
EOF
cat "$OUT/results.md"
