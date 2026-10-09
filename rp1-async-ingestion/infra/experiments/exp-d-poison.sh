#!/usr/bin/env bash
# Experiment (d): poison events -> DLT without blocking their partitions; then DLQ replay is harmless.
# Poison = subscriberKey "poison-*" makes the simulated downstream reject permanently (400).
# Expect: poison events in DLT (missingButInDlt == poison), everything else persisted, e2e latency normal,
# replaying the DLT re-DLTs the poison (still bad) and creates no duplicate rows.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -dims "$0" "$@"; fi
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-30}; DURATION=${DURATION:-60}; POISON=${POISON:-25}

require_jars
OUT="$ROOT/docs/results/stage5/d-poison/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$OUT/logs"
new_key; reset_env
start_ingest async "$OUT/logs"
start_consumer 0 "$OUT/logs" --rp1.downstream.fault-markers=true
loadgen_run "$OUT/run" --rate "$RATE" --duration "$DURATION" --batch-size 20 --poison "$POISON" --invalid-fraction 0.005
loadgen_verify "$OUT/run" --stable-s 15 --max-wait-s 300
curl -s localhost:8081/actuator/prometheus | grep -E '^rp1_consumer_e2e_latency_seconds_(max|count|sum)' > "$OUT/consumer-e2e.prom"
rows_before=$(psql_rp1 -c "SELECT count(*) FROM callback_event")
java -jar "$LOADGEN_JAR" dlq-replay --bootstrap "$KAFKA_DIRECT" --group "rp1-dlq-replay-$(date +%s)" > "$OUT/replay.json" 2>"$OUT/replay.log"
sleep 20
rows_after=$(psql_rp1 -c "SELECT count(*) FROM callback_event")
dups=$(psql_rp1 -c "SELECT count(*) FROM (SELECT event_id FROM callback_event GROUP BY 1 HAVING count(*)>1) d")
python3 - "$OUT" "$rows_before" "$rows_after" "$dups" <<'EOF' > "$OUT/results.md"
import json, sys
o, before, after, dups = sys.argv[1:5]; s = json.load(open(f"{o}/run/summary.json")); v = json.load(open(f"{o}/run/verify.json"))
print("# Experiment (d): poison events -> DLT, partitions unblocked; DLQ replay is idempotent\n")
for k in ["validEvents", "poisonEvents", "persistedDistinct", "missing", "missingButInDlt", "lostUnaccounted", "duplicateRows",
          "dltRecordsForRun", "invalidEventsGenerated", "invalidRoutedToTopic", "statusMismatches", "eventCaptureExcludingPoison"]:
    print(f"- {k}: {v[k]}")
print(f"- batchCaptureFirstAttempt: {s['batchCaptureFirstAttempt']}")
print("\nConsumer e2e latency (ingest -> processed), seconds:\n```\n" + open(f"{o}/consumer-e2e.prom").read() + "```")
print("\nDLQ replay:\n```\n" + open(f"{o}/replay.json").read() + "```")
print(f"- callback_event rows before replay: {before}, after: {after}, duplicate event ids: {dups}")
EOF
cat "$OUT/results.md"
