# Stage 1 — Flow

## Infra topology

```
            host (services / loadgen / gate tool)
                 │ localhost:39092-39094
                 ▼
            ┌──────────┐   latency / connection-cut faults injected here
            │ Toxiproxy│   (control API :8474)
            └──┬──┬──┬─┘
               ▼  ▼  ▼
          kafka-1 kafka-2 kafka-3     each broker is also a KRaft controller (:9093)
            replication between brokers on :29092 (not proxied)
               │
     kafka-init (runs once): creates the topics with explicit settings, then exits
     kafka-exporter → Prometheus (:9090) → Grafana (:3000)
     Postgres (5442 direct, 55432 via Toxiproxy) ← consumer, from Stage 3
```

## How client traffic reaches Kafka

A Kafka client first connects to a bootstrap address, then reconnects to whichever broker leads each partition,
using the address that broker *advertises*. If brokers advertised their real address, only the first connection
would pass through Toxiproxy and fault injection would be meaningless.

So each broker has four listeners:

| Listener | Port in container | Advertised as | Used by |
|---|---|---|---|
| CONTROLLER | 9093 | — | KRaft quorum between the 3 brokers |
| INTERNAL | 29092 | `kafka-N:29092` | Replication between brokers, `kafka-init`, `kafka-exporter` |
| PROXIED | 9092 | `localhost:3909x` (a Toxiproxy port) | Services and loadgen. Every connection goes through Toxiproxy. |
| DIRECT | 9094 | `localhost:4909x` | Debugging and the gate's verify step (bypasses Toxiproxy) |

Toxiproxy maps `39092 → kafka-1:9092`, `39093 → kafka-2:9092`, `39094 → kafka-3:9092`.

## Durability settings (and why)

| Setting | Value | Why |
|---|---|---|
| replication factor | 3 | Each message lives on 3 brokers |
| `min.insync.replicas` | 2 | An `acks=all` write needs the leader plus at least 1 follower, so it survives losing any one broker |
| `unclean.leader.election.enable` | false | Never promote a replica that is behind, because that would drop messages already confirmed |
| `auto.create.topics.enable` | false | A mistyped topic name fails loudly instead of creating a junk topic |
| `offsets.topic.replication.factor` | 3 | Consumer offsets survive a broker loss too |
| `restart` | `"no"` | Chaos tests kill brokers on purpose; nothing should bring them back silently |

## Gate flow (`infra/scripts/stage1-gate.sh`)

```
setup ─► 1 config ─► 2 baseline ─► 3 proxy proof ─► 4 kill kafka-2 mid-run ─► 4b check fencing
      ─► 5 degraded writes ─► 6 min-ISR enforcement ─► 7 kill kafka-3 (warm producer) ─► 7b cold producer
      ─► 8 restart + wait for full ISR ─► 9 verify zero loss ─► summary
```

| Step | What happens | Pass condition |
|---|---|---|
| setup | `compose up --wait` (`--fresh` wipes first), reset Toxiproxy, build the gate tool | Stack healthy |
| 1 | Describe every topic and broker config | Partitions, RF 3, min ISR, unclean off, every partition has 3 in-sync replicas; broker defaults on all 3 brokers |
| 2 | Send 1000 `acks=all` messages through Toxiproxy | All 1000 confirmed |
| 3a | Add 300ms latency in Toxiproxy on all 3 brokers, send 50 | Median ack time ≥ 300ms (proves traffic really goes through the proxy) |
| 3b | Disable all 3 proxies, send 5 | All 5 fail (proves there's no path around the proxy) |
| 4 | Send 6000 messages at 600/s; 3s in, `SIGKILL` kafka-2 | All 6000 confirmed |
| 4b | Describe the gate topic | No partition led by kafka-2; kafka-2 removed from every in-sync replica set |
| 5 | With 2 of 3 brokers up, send 1000 | All 1000 confirmed |
| 6 | Send 20 to `stage1-gate-minisr3` (needs 3 in-sync replicas, only 2 alive), no retries | All 20 rejected with `NotEnoughReplicasException` |
| 7 | Producer already running at 100/s; `SIGKILL` kafka-3 too (1 of 3 left, KRaft quorum lost) | No ack later than kafka-3's death + 1s (Docker's recorded exit time); sends after the kill fail |
| 7b | Start a *new* producer while quorum is lost; app-side deadline 15s | Nothing confirmed; the deadline bounds the wait |
| 8 | Start kafka-2 and kafka-3, poll | Every gate partition back to 3 in-sync replicas |
| 9 | Read the whole gate topic via the direct listener and diff it against the ledger | Zero confirmed messages missing |

### Ledger / verify logic

- Every message ID is `<run>:<phase>:<n>`, keyed `sub-0`…`sub-63` so traffic spreads over all partitions.
- `produce` writes `<phase>.acked` and `<phase>.failed` files.
- `verify` reports:
  - **acked-but-missing (LOSS)** must be 0. This is the pass condition.
  - **failed-but-present** is informational: the producer was told "failed" but the message is in Kafka anyway.
  - **duplicate IDs** in the log.
