# rp1-async-ingestion

Reference build for event-driven async callback ingestion: SFMC Event Notification Service webhooks → ingest
(validate, `acks=all` produce, ack within SFMC's 2s budget) → Kafka → idempotent consumers → Postgres. It includes a
**sync baseline mode** so the same code and load compare sync vs async, plus scripted experiments that produce
every number in the docs.

- Design and trade-offs: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Every edge case → behavior → test: [docs/EDGE_CASES.md](docs/EDGE_CASES.md)
- Interview Q&A with evidence links: [docs/DEFENSE.md](docs/DEFENSE.md)
- Raw results: [docs/results/](docs/results/) · Brief: [CLAUDE.md](CLAUDE.md) · Stage 1 notes: [documentation/](documentation/)

## Status

| Stage | Scope | State |
|---|---|---|
| 1 | Infra: 3-broker KRaft, Postgres, Toxiproxy, Prometheus, Grafana | gate 16/16 ([results](docs/results/stage1/)) |
| 2 | common + ingest-service (async + sync baseline) | gate: 29 unit + 5 IT green ([results](docs/results/stage2/)) |
| 3 | consumer-service: idempotent sink, retry topics, DLT, state guard | gate: kill -9, poison, rebalance, out-of-order ITs green ([results](docs/results/stage3/)) |
| 4 | loadgen + verifier | sync cliff reproduced on the real stack ([calibration](docs/results/stage5/a-cliff-calibration/)) |
| 5 | experiments (a)–(f) | [docs/results/stage5/](docs/results/stage5/) |
| 6 | metrics, dashboard, 19 alert rules, GitHub Actions | [alerts](infra/prometheus/alerts.yml), [dashboard](infra/grafana/dashboards/rp1-overview.json), [CI](../.github/workflows/rp1.yml) |
| 7 | docs | ARCHITECTURE / EDGE_CASES / DEFENSE |

## Layout

```
common/            envelope, canonical event id, event taxonomy + status ranks, error taxonomy (no Spring)
sink/              idempotent Postgres sink + monotonic status guard + simulated downstream (shared by both modes)
ingest-service/    POST /callbacks (async: produce+ack; 'sync' profile: process inline)
consumer-service/  consume -> sink; retry topics, DLT, throttle pause, lag/age monitor
loadgen/           SFMC-style sender + ledger, verifier, DLQ replay (plain Java CLI)
testkit/           Testcontainers Kafka-behind-Toxiproxy (test scope)
infra/             compose, topics, Toxiproxy, Prometheus (+alerts), Grafana (+dashboard), gate + experiment scripts
docs/              ARCHITECTURE, EDGE_CASES, DEFENSE, results/
```

## Run it (everything local and free: Docker, Java 21, Maven)

```sh
docker compose -f infra/docker-compose.yml up -d --wait    # Kafka x3, Postgres, Toxiproxy, Prometheus, Grafana
mvn -q install -DskipTests                                   # builds the three executable jars
mvn verify                                                   # unit + Testcontainers ITs (needs Docker, ~5 min)

export RP1_SIGNATURE_KEYS=$(openssl rand -base64 32) RP1_SIGNATURE_KEY=$RP1_SIGNATURE_KEYS   # never committed
java -jar ingest-service/target/ingest-service-0.1.0-SNAPSHOT.jar        # :8080 async (add --spring.profiles.active=sync for baseline)
java -jar consumer-service/target/consumer-service-0.1.0-SNAPSHOT.jar    # :8081
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar run --url http://localhost:8080/callbacks --rate 20 --duration 30 --out /tmp/run1
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar verify --run /tmp/run1 --jdbc jdbc:postgresql://localhost:5442/rp1 --bootstrap localhost:49092
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar dlq-replay --bootstrap localhost:49092 --dry-run
```

Experiments (each writes `docs/results/stage5/<name>/<timestamp>/`; they start and stop the services themselves):

```sh
infra/experiments/exp-a-cliff.sh [--ens-suspension]   # sync vs async rate sweep
infra/experiments/exp-b-broker-kill.sh                 # broker killed mid-run
infra/experiments/exp-c-toxiproxy.sh                   # latency then cut on the Kafka link
infra/experiments/exp-d-poison.sh                      # poison -> DLT, partition unblocked, replay is idempotent
infra/experiments/exp-e-ordering-rebalance.sh          # joins + kill -9 during load, final-state check
infra/experiments/exp-f-lag.sh                         # 1.5x capacity: lag, oldest-message age, drain time
```

| What | Address |
|---|---|
| Ingest / consumer(s) | `:8080` / `:8081`..`:8084` (`/actuator/health`, `/actuator/prometheus`) |
| Kafka via Toxiproxy (**services**) | `localhost:39092,39093,39094` |
| Kafka direct (tools/verification) | `localhost:49092,49093,49094` |
| Postgres direct / via Toxiproxy | `localhost:5442` / `localhost:55432` (db/user/pass `rp1`) |
| Toxiproxy API · Prometheus · Grafana | `:8474` · `:9090` (alerts: `/alerts`) · `:3000` (admin/admin, dashboard "RP1 async ingestion") |

Host ports avoid 9092–9094 / 5432 so the stack runs next to other local Kafka/Postgres setups.

## Topics

Created by [infra/kafka/create-topics.sh](infra/kafka/create-topics.sh); all RF=3, `min.insync.replicas=2`,
`unclean.leader.election.enable=false`; broker auto-create off.

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `callback-events` | 12 | 7d | main stream, keyed by subscriberKey |
| `callback-events-retry-0..2` | 12 | 7d | non-blocking retries (1s/2s/4s + jitter) |
| `callback-events-dlt` | 3 | 14d | dead letters (replay with `loadgen dlq-replay`) |
| `invalid-events` | 3 | 14d | per-event validation failures inside an accepted batch |
| `stage1-gate`, `stage1-gate-minisr3` | 12, 3 | 1d | Stage 1 gate only |

## Cost

Everything runs locally on Docker (Docker Desktop's free personal-use tier; Colima works too) with pinned
open-source images. CI uses GitHub-hosted runners, which are free for this public repository. No cloud accounts,
registries or paid services.
