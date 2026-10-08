#!/usr/bin/env bash
# Creates RP1 topics with explicit durability settings (never rely on broker defaults alone).
# Idempotent: safe to re-run.
set -euo pipefail

KT=/opt/kafka/bin/kafka-topics.sh
: "${BOOTSTRAP:?BOOTSTRAP not set}"

DAY_MS=86400000
# Applied to every topic. RF=3 + min ISR 2 + no unclean election = an acks=all write survives any single broker loss.
COMMON=(--replication-factor 3
        --config min.insync.replicas=2
        --config unclean.leader.election.enable=false)

create() {  # name partitions retention_days
  local name=$1 parts=$2 days=$3
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$parts" "${COMMON[@]}" \
    --config retention.ms=$((days * DAY_MS))
  echo "ok: $name partitions=$parts retention=${days}d"
}

# Main stream. 12 partitions fixed up front: adding partitions later remaps key->partition and breaks
# per-subscriber ordering for in-flight keys. 12 divides evenly across 1/2/3/4/6/12 consumers and 3 brokers.
# 7d retention >> worst-case consumer outage + drain time.
create callback-events        12 7
# Spring Kafka's default dead-letter suffix is "-dlt". Longer retention: humans triage/replay it.
create callback-events-dlt     3 14
# Events that failed per-event validation inside an otherwise valid batch (ingest side).
create invalid-events          3 14
# Gate-only topic: same config as callback-events so stage-1 chaos traffic never pollutes the real stream.
create stage1-gate            12 1

# Gate-only: min ISR 3 so a single broker loss MUST reject acks=all writes (proves the broker enforces min ISR).
"$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic stage1-gate-minisr3 --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=3 --config unclean.leader.election.enable=false \
  --config retention.ms=$DAY_MS
echo "ok: stage1-gate-minisr3"

# Retry topics (callback-events-retry-*) are created by the consumer in stage 3 once the backoff
# schedule is fixed; auto-create is off, so they must be declared with RF=3 there.
