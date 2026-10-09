#!/usr/bin/env bash
# Experiment (e): per-subscriber ordering / final-state correctness across rebalances (async).
# Consumers join at t=15s and t=30s, one is kill -9'd at t=45s, then another joins. Hot-key traffic included.
# Expect: zero lost, zero duplicate rows, zero status mismatches (late events never regress state).
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -dims "$0" "$@"; fi
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-40}; DURATION=${DURATION:-75}

require_jars
OUT="$ROOT/docs/results/stage5/e-ordering-rebalance/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$OUT/logs"
new_key; reset_env
start_ingest async "$OUT/logs"
start_consumer 0 "$OUT/logs"
loadgen_run "$OUT/run" --rate "$RATE" --duration "$DURATION" --batch-size 20 --hot-fraction 0.05 &
LG=$!
sleep 15; start_consumer 1 "$OUT/logs"; echo "t=15 consumer-1 joined" > "$OUT/events.txt"
sleep 15; start_consumer 2 "$OUT/logs"; echo "t=30 consumer-2 joined" >> "$OUT/events.txt"
sleep 15; kill -9 "$CONSUMER_PID_1"; echo "t=45 consumer-1 kill -9" >> "$OUT/events.txt"; log "kill -9 consumer-1"
sleep 10; start_consumer 3 "$OUT/logs"; echo "t=55 consumer-3 joined" >> "$OUT/events.txt"
wait $LG
loadgen_verify "$OUT/run" --stable-s 15 --max-wait-s 600
python3 - "$OUT" <<'EOF' > "$OUT/results.md"
import json, sys
o = sys.argv[1]; s = json.load(open(f"{o}/run/summary.json")); v = json.load(open(f"{o}/run/verify.json"))
print("# Experiment (e): ordering / final state across rebalances and a kill -9 (async)\n")
print("```\n" + open(f"{o}/events.txt").read() + "```")
for k in ["validEvents", "persistedDistinct", "lostUnaccounted", "duplicateRows", "messagesFullyPersistedChecked", "statusMismatches", "eventCapture"]:
    print(f"- {k}: {v[k]}")
print(f"- batchCaptureFirstAttempt: {s['batchCaptureFirstAttempt']}")
EOF
cat "$OUT/results.md"
