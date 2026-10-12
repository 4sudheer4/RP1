# RP1 — Stage 1b addendum: prod-grade Kafka reference on Kubernetes (Strimzi, zero cost)

Append to CLAUDE.md. Compose stays as the fast dev loop; this adds a prod-style reference that runs locally on kind. **No cloud, no paid services, no accounts.**

## Goal
Show the Kafka layer as it would be run in production: dedicated controllers/brokers, AZ-aware replica placement, TLS + SASL/SCRAM + ACLs, topics as code, real observability, and a rerun of the Stage 1 durability gate against it.

## Deliverables (`prod/`)
- `prod/kind/`: kind cluster config, 3+ workers labeled `topology.kubernetes.io/zone=az-a|b|c`. Script to create/destroy.
- `prod/strimzi/`: install notes (pinned Strimzi version), `Kafka` CR (KRaft), `KafkaNodePool` x2 (3 controllers, 3 brokers), `rack.topologyKey: topology.kubernetes.io/zone`, TLS listeners + SCRAM auth, authorization (simple ACLs), JMX metrics config, PodDisruptionBudgets, anti-affinity.
- `prod/strimzi/topics/`: `KafkaTopic` CRs for every topic (same RF/min-ISR/retention/partitions as create-topics.sh; 12 partitions on callback-events, explained).
- `prod/strimzi/users/`: `KafkaUser` CRs with least-privilege ACLs: ingest = write callback-events + invalid-events; consumer = read callback-events, group, write retry/DLT topics; admin separate.
- `prod/overlays/local/`: reduced heaps/replicas so it fits a laptop. Document exactly what differs from prod sizing and why.
- `prod/client-configs/`: producer + consumer profiles (acks=all, idempotence, compression, client.rack, delivery/request timeouts sized to the 2s ingest deadline, cooperative assignor, static membership).
- `prod/observability/`: Prometheus rules for UnderMinIsrPartitionCount, UnderReplicatedPartitions, OfflinePartitionsCount, ActiveControllerCount, consumer lag, oldest-message age. Grafana dashboard JSON.
- `prod/gate/`: Stage 1 gate adapted to kubectl: delete a broker pod mid-produce, drain an "AZ" node, two-broker loss, recovery, ledger verify. Same pass/fail rigor, results under `docs/results/stage1b/`.
- `docs/PROD_DEPLOYMENT.md`: every non-default setting with a one-line rationale; capacity formula (throughput x retention x RF + headroom); rolling-restart procedure gated on ISR health; upgrade, DR (MirrorMaker 2), quotas; **explicit list of what cannot be verified locally** (real multi-AZ network, real disks, scale).

## Rules
- Pin every version. No plaintext listeners in `prod/`. No default/shared credentials committed; generate via Strimzi secrets.
- Run everything you claim: create the kind cluster, apply manifests, run the gate, report real output. Say plainly what you did not run or could not fit in laptop memory.
- Stop at gates: (1) cluster up + all pods ready, (2) topics/users reconciled, ACLs enforced (negative test: unauthorized write rejected), (3) gate passes, (4) docs.
- Small commits. Concise summaries; no giant code dumps in chat.
