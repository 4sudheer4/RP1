#!/usr/bin/env bash
# Experiment (b): kill a broker mid-run (async mode). Expect: zero lost events, zero duplicate rows, zero status
# mismatches; some 503s during leader failover that SFMC-style retries recover.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -i "$0" "$@"; fi
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-40}; DURATION=${DURATION:-90}; KILL_AT=${KILL_AT:-30}; RESTART_AT=${RESTART_AT:-60}; BROKER=${BROKER:-kafka-2}

require_jars
OUT="$ROOT/docs/results/stage5/b-broker-kill/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$OUT/logs"
new_key; reset_env
start_ingest async "$OUT/logs"
start_consumer 0 "$OUT/logs"
loadgen_run "$OUT/run" --rate "$RATE" --duration "$DURATION" --batch-size 20 &
LG=$!
sleep "$KILL_AT"; log "docker kill rp1-$BROKER-1"; docker kill "rp1-$BROKER-1" >/dev/null
echo "killed_at=$(docker inspect -f '{{.State.FinishedAt}}' "rp1-$BROKER-1")" > "$OUT/events.txt"
sleep $((RESTART_AT - KILL_AT)); log "docker start rp1-$BROKER-1"; docker start "rp1-$BROKER-1" >/dev/null
echo "restarted_at=$(date -u +%FT%TZ)" >> "$OUT/events.txt"
wait $LG
loadgen_verify "$OUT/run" --stable-s 15 --max-wait-s 600
ensure_brokers
python3 - "$OUT" <<'EOF' > "$OUT/results.md"
import json, sys
o = sys.argv[1]; s = json.load(open(f"{o}/run/summary.json")); v = json.load(open(f"{o}/run/verify.json"))
print("# Experiment (b): broker killed mid-run (async)\n")
print(open(f"{o}/events.txt").read())
for k in ["batches", "batchCaptureFirstAttempt", "batchCaptureEventual", "batchesDropped", "statusCounts", "firstAttemptLatencyMs"]:
    print(f"- {k}: {s[k]}")
for k in ["validEvents", "persistedDistinct", "lostUnaccounted", "duplicateRows", "statusMismatches", "messagesFullyPersistedChecked", "eventCapture"]:
    print(f"- {k}: {v[k]}")
EOF
cat "$OUT/results.md"
