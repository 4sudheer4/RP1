# rp1-async-ingestion

Reference build for event-driven async callback ingestion (SFMC-style webhooks -> Kafka -> idempotent consumers).
Design brief and working agreements: [CLAUDE.md](CLAUDE.md).

## Status

| Stage | Scope | State |
|---|---|---|
| 1 | Infra: 3-broker KRaft, Postgres, Toxiproxy, Prometheus, Grafana | gate passed 16/16 (run 2) — notes in [documentation/](documentation/), logs in [docs/results/stage1/](docs/results/stage1/) |
| 2 | common + ingest-service | not started |
| 3–7 | consumer, loadgen, experiments, observability/CI, docs | not started |

## Run the infra

```sh
docker compose -f infra/docker-compose.yml up -d --wait
infra/scripts/stage1-gate.sh            # stage-1 gate; add --fresh to start from an empty cluster
docker compose -f infra/docker-compose.yml down -v
```

Requires Docker, Java 21, Maven (the gate builds a small kafka-clients tool in `infra/gate/`).

| What | Host address | Notes |
|---|---|---|
| Kafka via Toxiproxy (**services use this**) | `localhost:39092,localhost:39093,localhost:39094` | every broker connection is proxied, so toxics hit all of it |
| Kafka direct (debug / verification) | `localhost:49092,localhost:49093,localhost:49094` | bypasses Toxiproxy |
| Postgres direct | `localhost:5442` db/user/pass `rp1` | |
| Postgres via Toxiproxy | `localhost:55432` | for DB-latency experiments |
| Toxiproxy API | `http://localhost:8474` | proxies `kafka-1..3`, `postgres` |
| Prometheus | `http://localhost:9090` | scrapes kafka-exporter, and ingest (8080) / consumer (8081) on the host |
| Grafana | `http://localhost:3000` | admin/admin; Prometheus datasource provisioned |
| kafka-exporter | `http://localhost:9308/metrics` | ISR, partition, consumer-lag metrics |

Host ports deliberately avoid 9092–9094 / 5432 so the stack runs next to other local Kafka/Postgres setups.

## Topics

Created by [infra/kafka/create-topics.sh](infra/kafka/create-topics.sh); all RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false`.
Broker auto-create is off.

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `callback-events` | 12 | 7d | main stream, keyed by subscriber |
| `callback-events-dlt` | 3 | 14d | dead letters (Spring's default `-dlt` suffix) |
| `invalid-events` | 3 | 14d | per-event validation failures inside a valid batch |
| `stage1-gate`, `stage1-gate-minisr3` | 12, 3 | 1d | gate-only; keeps chaos traffic off the real stream |
