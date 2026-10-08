# Stage 1 — Infra: summary

Date: 2026-10-07

**Status:** infra built; gate run 2 passed **16/16** (run 1: 15/16, the one failure was a bug in the gate script, since fixed).
Run 2 had an incident: the Mac went into idle sleep during step 7 (details in [stage1-results.md](stage1-results.md)).
That check still holds up, but one clean re-run with the laptop plugged in is recommended before calling Stage 1 closed.

## Brief's gate for Stage 1

> Topics created with correct RF/min-ISR; kill one broker, produce with `acks=all` still succeeds.

Both are met (steps 1 and 4 of the gate). The gate also covers more than the brief asked: Toxiproxy-in-path,
min-ISR enforcement, two-broker loss, recovery, and a zero-loss verification of every acked message.

## What was built

| Piece | File | What it does |
|---|---|---|
| Brief | `CLAUDE.md` | The project brief, saved in the repo as the brief instructs |
| Infra stack | `infra/docker-compose.yml` | 3 Kafka brokers (KRaft, Kafka 3.9.0), Postgres 16.4, Toxiproxy 2.9.0, Prometheus 2.54.1, Grafana 11.2.0, kafka-exporter 1.8.0. All image versions pinned. |
| Topics | `infra/kafka/create-topics.sh` | Run once by the `kafka-init` container. Creates topics with explicit settings (see below). Safe to re-run. |
| Fault injection | `infra/toxiproxy/toxiproxy.json` | One proxy per broker plus one for Postgres |
| Metrics | `infra/prometheus/prometheus.yml`, `infra/grafana/` | Prometheus scrapes kafka-exporter now, and the ingest (8080) / consumer (8081) services on the host once they exist. Grafana has the Prometheus datasource provisioned; dashboards come in Stage 6. |
| Gate tool | `infra/gate/` (`Stage1Gate.java`) | Small plain kafka-clients program. `produce` sends `acks=all` messages and writes a ledger of confirmed and failed message IDs. `verify` reads the whole topic back and diffs it against the ledger. |
| Gate script | `infra/scripts/stage1-gate.sh` | Runs every check in order, kills brokers, drives Toxiproxy, saves output to `docs/results/stage1/<run>/` |
| README | `README.md` | Ports, topics, how to run |

## Topics

All topics: replication factor 3, `min.insync.replicas=2`, `unclean.leader.election.enable=false`.
Broker-level auto topic creation is off, so a mistyped topic name fails instead of silently creating a topic.

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `callback-events` | 12 | 7 days | Main stream, keyed by subscriber |
| `callback-events-dlt` | 3 | 14 days | Dead letters (Spring Kafka's default `-dlt` suffix) |
| `invalid-events` | 3 | 14 days | Single bad events from an otherwise valid batch |
| `stage1-gate` | 12 | 1 day | Gate only: same settings as `callback-events`, keeps test traffic off the real stream |
| `stage1-gate-minisr3` | 3 | 1 day | Gate only: needs 3 in-sync replicas, used to prove min ISR is enforced |

Why 12 partitions: the partition count can't be raised later without changing which partition a subscriber's
events land on (that breaks per-subscriber ordering). 12 splits evenly across 1, 2, 3, 4, 6 or 12 consumers and 3 brokers.

Retry topics (`callback-events-retry-*`) are not created yet. The consumer creates them in Stage 3 once the backoff
schedule is fixed, and must set replication factor 3 explicitly since auto-create is off.

## Ports

Another local project (sfmc-kafka) already uses 9092–9094, so this stack uses different ports.

| What | Address |
|---|---|
| Kafka via Toxiproxy (**services use this**) | `localhost:39092,39093,39094` |
| Kafka direct (debugging / verification) | `localhost:49092,49093,49094` |
| Postgres direct | `localhost:5442` (db / user / password: `rp1`) |
| Postgres via Toxiproxy | `localhost:55432` |
| Toxiproxy control API | `http://localhost:8474` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (admin / admin) |
| kafka-exporter | `http://localhost:9308/metrics` |

## How to run

```sh
docker compose -f infra/docker-compose.yml up -d --wait     # start (about 20s)
infra/scripts/stage1-gate.sh --fresh                         # gate from an empty cluster (about 4 min)
docker compose -f infra/docker-compose.yml down -v           # stop and wipe
```

Needs Docker, Java 21 and Maven.

## Git state

Committed:
1. `a28edab` Add project brief as CLAUDE.md and gitignore
2. `a6b1722` Add stage-1 infra: 3-broker KRaft, Postgres, Toxiproxy, Prometheus, Grafana
3. `21c9216` Add stage-1 gate: topic config checks and acks=all under broker loss

Not yet committed: `README.md`, the gate-script fixes (kill time from Docker, `caffeinate`), the `docs/results/stage1/`
logs, and this `documentation/` folder.

## What's left for Stage 1

- One clean gate run with the laptop on power (the script now holds a no-sleep assertion with `caffeinate -i`).
- Commit the remaining files.
