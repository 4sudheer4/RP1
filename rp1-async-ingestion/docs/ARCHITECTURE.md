# Architecture

## 1. The constraint we design around (SFMC Event Notification Service)

From the official ENS docs (read via the mirror of the Marketing Cloud API docs, July 2026 collection, since
developer.salesforce.com blocks automated fetches):

| Fact | Consequence here |
|---|---|
| The callback has **2 seconds** to return 200/201/202/203/204 (*Retries and Callback Suspensions*) | Handler hard deadline 1.5s, Kafka client timeouts nested inside it. The brief said 3s; the docs say 2s, so 2s is used. |
| Otherwise ENS **retries up to 3 times, then drops the batch** | A slow 200 costs the same as a 5xx. Fail fast, and never make a batch permanently fail. |
| **2+ consecutive failures in 5 min suspend the callback for 10 min** (growing to 1h); events are dropped while suspended | Sync overload causes data loss, not just latency. Modeled in loadgen with `--suspend-after` (time-scaled). |
| ENS is **at-least-once** (*Get Started*) | Duplicates are normal input. Idempotency is mandatory, not an optimization. |
| Batches of up to `maxBatchSize` (100..1000) events, a JSON array | Body limit 2 MiB, batch limit 1000 events. |
| `x-sfmc-ens-signature` = base64 HMAC-SHA256 over the payload, key per callback, `ens-regenerate` **deactivates the old key immediately** | HMAC over the raw bytes, constant-time compare, accept current + previous key during a config rollout. |
| Callback verification: unsigned POST `{callbackId, verificationKey}`, verify via `/platform/v1/ens-verify` within 4h | Accepted only while `rp1.ingest.verification.enabled`; details logged for manual verification (the docs' suggested pattern). |
| No documented unique event id (`compositeId` is "deprecated"); `timestampUTC` is epoch millis | Dedup key = SHA-256 of the canonical event JSON. Timestamps are untrusted for ordering beyond a skew bound. |

**Not verified against a live SFMC account:** whether the signature key is used as UTF-8 bytes or base64-decoded
(the docs don't say, and open-source implementations disagree). Configurable via `RP1_SIGNATURE_KEY_ENCODING`,
default UTF8. The retry backoff ENS uses between attempts is also undocumented; loadgen uses 1s/2s/4s.

## 2. Shape

```
SFMC ENS ──POST /callbacks (≤1000 events, 2s budget)──► ingest-service
                                                         │ bulkhead → 415/413 → HMAC/handshake → parse → per-event validate
                                                         │ produce each event (key = subscriberKey), acks=all, idempotent
                                                         │ await all futures ≤ 1.5s ──► 200 | 503 + Retry-After
                                                         ▼
                      Kafka (3 brokers, RF3, min.insync 2, no unclean election)
   callback-events (12p) ── invalid-events (3p) ── callback-events-retry-0..2 (12p) ── callback-events-dlt (3p)
                                                         ▼
                                                  consumer-service (cooperative-sticky group)
                                                         │ one Postgres txn per event:
                                                         │   INSERT callback_event ON CONFLICT DO NOTHING   (dedup arbiter)
                                                         │   downstream call (simulated DE API, fixed capacity)
                                                         │   guarded UPSERT message_status                (monotonic)
                                                         │ commit → offset commit (after txn)
                                                         │ transient → retry topics (backoff+jitter) → DLT ; permanent → DLT
                                                         ▼
                                                      Postgres
```

`sink` is shared by the consumer and by ingest's **sync baseline** (`--spring.profiles.active=sync`), which runs
the same business code inline before responding. That makes the sync vs async comparison apples to apples.

## 3. The ack contract

**200 means: every event in the batch (valid ones on `callback-events`, invalid ones on `invalid-events`) has been
acknowledged by Kafka with `acks=all` and `min.insync.replicas=2`.** Nothing more: not processed, not in Postgres.

- Futures for the whole batch are awaited together. No per-request `flush()`; linger is 5ms.
- Nothing is buffered past the response. If Kafka can't confirm in time, the answer is 503, and SFMC holds
  the data and retries. **No ack-and-buffer in memory, and no local disk spool.** A spool would turn a stateless
  service into a durable store with its own replication, recovery and replay problems, to cover a window that SFMC
  already covers with retries. It's out of scope on purpose.
- **A 503 does not mean "not written".** Stage 1 measured 183 and 325 sends reported failed that were in the log.
  The retry then duplicates them, and the consumer's primary key removes the duplicates.
- What can still lose an acked event: losing ≥2 of 3 replicas of a partition simultaneously with unflushed page
  cache (RF3 + min ISR 2 tolerates one), or an operator enabling unclean leader election.

## 4. Deadline budget (ingest)

| Setting | Value | Why |
|---|---|---|
| SFMC timeout | 2000ms | official |
| `rp1.ingest.hard-deadline` | 1500ms | leaves ~500ms for network/TLS and SFMC-side measurement |
| `delivery.timeout.ms` | 1200 | must be ≥ `linger.ms` + `request.timeout.ms` (client-enforced); < deadline so the future fails first |
| `request.timeout.ms` | 1000 | one broker round trip under failover |
| `max.block.ms` | 500 | bounds `send()` blocking on metadata / full buffer |
| `min-produce-budget` | 100ms | below this we don't start producing: we couldn't finish anyway |
| app-level wait | `allOf(futures).get(remaining)` | the backstop. Stage 1 finding #2: an idempotent producer waiting for a producer id during quorum loss ignores `delivery.timeout.ms`. |

Circuit breaker (count window 20, 50% failure or 80% slow-calls >1s) opens to instant 503s, half-opens after 5s.
Bulkhead: 64 in-flight requests. Tomcat accept-count 50, so excess load is shed with 503 instead of queueing past SFMC's timeout.

## 5. Partitioning and ordering

- **Key = `info.subscriberKey`**, never the batch id. A batch is split into per-event records at produce time,
  so one subscriber's events stay in order within a partition.
- **12 partitions, fixed up front.** Adding partitions remaps `hash(key) % n`, so in-flight keys would split across
  two partitions and lose ordering. 12 divides evenly across 1/2/3/4/6/12 consumers.
- **The guarantee is weaker than "ordered", deliberately:** SFMC retries, retry topics and DLT replay all
  reorder. Correctness therefore doesn't depend on order: the status projection is a max over a total order
  (rank, ordering timestamp, event id). Experiment (e) and the IT with shuffled double delivery check it.
- Hot subscriber: one key always maps to one partition, which caps it at one consumer thread. The hot-key run in (e)
  stresses this. The 10x answer is in DEFENSE.md.

## 6. Event identity and ordering timestamp

- `eventId` = upstream `eventId` if present (`u:` prefix), else SHA-256 of canonical JSON (keys sorted recursively,
  no whitespace). Retries of the same event hash the same; different events differ. Limitation: `1` vs `1.0`.
- `orderingTs` = `timestampUTC` clamped to `receivedAt + 5 min`, so a skewed future timestamp can't win ties forever.
  It's used only to break ties within the same status rank. Rank always dominates.

## 7. Consumer

- At-least-once. One Postgres transaction per event: the event-log insert (`ON CONFLICT (event_id) DO NOTHING`)
  is the dedup record and the business write in one. Zero rows inserted means duplicate, so we stop.
- The container commits offsets after the listener returns (`ack-mode: batch`), i.e. after the transaction committed.
  A crash anywhere before that redelivers, and the redelivery is a no-op. Proven with `kill -9` of the packaged jar.
- Two consumers racing on one event serialize on the primary key: the second insert blocks on the first
  transaction, then conflicts (IT `concurrentConsumersSameEventExactlyOneApplies`).
- **The downstream side effect is at-least-once.** If the process dies after the call and before commit, the call
  repeats. The downstream must be idempotent on `eventId` (the real DE API upserts by key). `downstream_call` counts
  repeats. Transactional outbox was considered: it moves the call after the commit but still at-least-once, so it
  adds machinery without changing the guarantee.
- Session timeout 10s (default 45s): faster redelivery after a crash. Cooperative-sticky assignor: rebalances
  move only the partitions that must move.
- `max-poll-records` 50 × worst case (~1s downstream wait + ~1s rate-limit wait + DB) is ≪ `max.poll.interval.ms`
  300s, so slow processing can't trigger rebalance storms.

## 8. Failures, retries, DLT

| Error | Class | Path |
|---|---|---|
| downstream 5xx / timeout, DB connection loss, deadlock | retryable | `callback-events-retry-0/1/2` (1s, 2s, 4s ±jitter, max 8s), then DLT |
| downstream 429 / saturation | retryable + **pause** main container (jittered exponential, ≤15s) | same |
| downstream 4xx, constraint violation (not the dedup key), unsupported envelope schema | non-retryable | DLT directly |
| bytes that aren't an envelope | `ErrorHandlingDeserializer` → non-retryable | DLT with original bytes |

Retries are **non-blocking**. A blocking retry stalls every subscriber behind the bad record on that
partition, and at the 2s SFMC scale a stalled partition is an outage. The cost is reordering, which the status
guard absorbs. DLT records carry Spring's `kafka_original-topic/-partition/-offset/-timestamp` and
`kafka_exception-*` headers plus `rp1-attempts`, `rp1-first-failure-ts` and `rp1-error-class`.

**Replay:** `loadgen dlq-replay` re-publishes DLT records to `callback-events` with the original key. It commits its
group offset only after an `acks=all` re-publish, so a crash re-replays and never skips. Replaying is always safe:
applied ids are no-ops, and old events can't regress status.

## 9. Backpressure

- **Ingest:** bulkhead + breaker + small accept queue mean SFMC sees a fast 503 (and retries later), never a hang.
- **Consumer:** Kafka itself is the buffer. The consumer pulls at downstream speed. On a 429 it pauses the
  container (heartbeats continue, no rebalance), and an optional token bucket keeps it under a published limit.

## 10. Lag, retention, drain budget

- Alerts on **lag** (records) **and age of the oldest unprocessed record**, read at the committed offset by
  `LagAgeMonitor`. 50k records could be 30s or 3h of work.
- Drain time ≈ backlog / (consumer capacity − arrival rate). Measured in experiment (f).
- Retention: `callback-events` 7 days ≫ worst-case outage + drain. Retry topics 7d, DLT 14d (humans triage).
  Invalid events 14d.

## 11. Observability

Micrometer → Prometheus (`/actuator/prometheus`) → Grafana dashboard `infra/grafana/dashboards/rp1-overview.json`.
Alert rules: `infra/prometheus/alerts.yml` (19 rules, `promtool`-checked in CI). Structured JSON logs (logstash
encoder) carry `traceId` (W3C `traceparent` or generated) and `batchId`. Payloads are never logged, and
subscriber keys are redacted (`h:` + 12 hex of SHA-256). Spring's default producer listener logged record keys on
failure, so it's replaced.

## 12. Non-default settings (one line each)

| Where | Setting | Rationale |
|---|---|---|
| topic | RF 3, `min.insync.replicas` 2, `unclean.leader.election.enable` false | acks=all survives one broker loss; never elect a replica missing acked data |
| broker | `auto.create.topics.enable` false | a typo'd topic fails loudly |
| producer | `acks=all`, `enable.idempotence=true`, `max.in.flight=5` | durable ack; broker dedups producer retries; order kept |
| producer | `compression.type=lz4`, `batch.size=64KB`, `linger.ms=5` | cheap CPU, smaller JSON on the wire; small linger because we wait on acks in-request |
| producer | timeouts in §4 | nested inside the 1.5s deadline |
| consumer | `enable.auto.commit=false`, `ack-mode=batch` | commit only after the txn |
| consumer | `CooperativeStickyAssignor`, `session.timeout.ms=10000`, `heartbeat.interval.ms=3000` | incremental rebalances; fast failure detection |
| consumer | `max.poll.records=50` | bound per-poll work vs `max.poll.interval.ms` |
| consumer | `ErrorHandlingDeserializer` | corrupt bytes go to the DLT, not into a crash loop |
| ingest | Tomcat `accept-count=50`, `connection-timeout=2s` | shed instead of queueing past SFMC's timeout; drop slow-loris bodies |
| ingest | `server.shutdown=graceful`, 10s phase | drain in-flight requests (each ≤1.5s) before closing the producer |
| sync profile | Hikari 60, downstream acquire-timeout 60s, no bulkhead | reproduce the old design faithfully; 60 < Postgres max_connections 100 |

## 13. Alternatives considered

- **Scale sync workers.** That only moves the cliff. Capacity is bounded by the downstream (DE API), not by our
  threads, and above it retries amplify load. See DEFENSE.md and experiment (a).
- **Ack before producing (fire-and-forget).** Turns any Kafka hiccup into silent loss of acknowledged data.
- **Kafka transactions / EOS.** Exactly-once inside Kafka doesn't cover the Postgres write or the downstream call.
  An idempotent sink gives the effect we need with fewer moving parts.
- **Disk spool on ingest.** See §3.
- **Partition by batch id.** It would scatter a subscriber's events across partitions and break ordering.
