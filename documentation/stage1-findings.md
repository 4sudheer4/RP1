# Stage 1 — Findings and what they mean for later stages

## 1. A "failed" send can still end up in Kafka

**Measured:** in run 1, 183 messages were reported as failed but are in the log. In run 2 the count was 325.

**Why:** when the leader writes a message but can't confirm replication in time (here, because brokers died),
the producer gets a timeout and reports failure. But the write may already be on the leader. Once the other
replicas recover, that write becomes durable.

**Meaning for Stage 2 (ingest):** returning a 5xx when Kafka is unhealthy is correct, but some of that batch may
already be in Kafka. SFMC will retry the whole batch, so **duplicates are guaranteed**, not just possible.

**Meaning for Stage 3 (consumer):** the idempotent sink (a unique event ID in Postgres, `INSERT … ON CONFLICT DO NOTHING`)
isn't optional. These measured counts show it's required.

**Interview line:** "A 200 means it's in Kafka. A 5xx does *not* mean it isn't. The retry turns that uncertainty
into a duplicate, and the consumer's unique constraint removes it."

## 2. `delivery.timeout.ms` doesn't always limit how long a send waits

**Observed:** a newly started idempotent producer hung indefinitely while the Kafka cluster had lost its controller
quorum, even with `delivery.timeout.ms=10000`. The thread dump showed it stuck in
`Sender.maybeSendAndPollTransactionalRequest`, waiting for a producer ID. Messages waiting for that ID never expire.

**Meaning for Stage 2:** the ingest handler's ≤2s deadline must be enforced by the application itself, with a
bounded wait on the produce futures. It can't rely on Kafka client timeouts alone. This matters most at startup
and after a producer gets recreated during an outage.

**Mitigation to consider:** create the producer and check it's ready at startup, and only report the service as
ready once the producer has its ID.

## 3. Broker failover takes seconds, far beyond the 2s deadline

**Measured:** with one broker killed mid-stream, the 99th-percentile ack time rose to **9.5s (run 1) / 8.5s (run 2)**,
against a normal median of about 100–190ms in the same phase. Nothing was lost; the messages were just slow while the
cluster noticed the dead broker and moved partition leadership.

**Why:** KRaft only fences a dead broker after its session timeout (`broker.session.timeout.ms`, 9s by default).
Until then, partitions it led have no working leader, and partitions it followed wait on it before confirming.

**Meaning for Stage 2:** during a broker failure, some requests will hit the 2s deadline. The correct response
is a fast 503 so SFMC retries later, not hanging until SFMC's 3s timeout. The circuit breaker around produce should
open quickly here.

**Option to evaluate in Stage 5:** lowering `broker.session.timeout.ms` shortens the window but risks fencing
healthy brokers during GC pauses. Measure before changing it.

## 4. Losing quorum is a full stop, not slow degradation

**Measured:** with 2 of 3 brokers dead, nothing was confirmed (phases G and H). The last ack came 100ms before the
second broker died; every send after that failed.

**Meaning:** the RF=3 / min ISR 2 setup tolerates exactly one broker failure. With two failures, ingest must
return 503 for everything, and SFMC's own retries are what get the data in later. Never accept a batch and hold
it in memory; that's the brief's "never ack-and-buffer" rule.

## 5. Test-harness lessons (keep for Stages 4–5)

- Take event times from the system of record (Docker's `State.FinishedAt`, the producer's own ack times), not from
  timestamps taken around CLI calls. `docker compose` takes about 3.4s per call on this machine.
- Keep the laptop awake for every timed experiment (`caffeinate -i`), and preferably plugged in. Idle sleep happened
  on battery in the middle of a run.
- Keep experiment traffic on separate topics (`stage1-gate`) so the real stream stays clean for later stages.
