# RP1 — Async Callback Ingestion (prod-grade reference build)

Paste/save as `CLAUDE.md` in the new repo (`rp1-async-ingestion`). Read fully before writing code.

## 1. Why this exists
Interview-prep project for a senior backend role (payments/messaging: Mastercard, Visa, Twilio, Stripe). It backs this resume line, which I must be able to defend at staff level:

> Re-architected payment-notification infrastructure from synchronous to event-driven async ingestion — decoupled callback acknowledgment from processing via a queue, eliminating retry exhaustion and improving callback capture reliability from ~70% to 99.9%.

Real-world story: Salesforce Marketing Cloud (SFMC) pushes event callbacks (webhooks) to our endpoint in batches with a **3s timeout per batch**. The old sync handler did all work (Data Extension reads/writes, downstream calls) before responding, regularly exceeded 3s -> SFMC saw failure -> retried -> more load -> retry exhaustion -> lost callbacks (~70% capture). Fix: endpoint validates, hands the batch to Kafka, acks immediately; consumers process asynchronously.

**Honesty rule:** this build produces *its own* measured numbers. Never claim or hard-code the resume numbers (70%/99.9%). Every number in docs must come from a run you executed. State clearly what was verified vs not.

## 2. What the prior simulation proved (and didn't)
`sim.py` (asyncio, in-process, Queue as Kafka stand-in), 8 workers (~111 batches/s capacity), 3s timeout scaled to 150ms, 3 retries:
- Sync: 100% capture at 95/s, **31% at 105/s**, 14% at 130/s. A cliff, not a slope: retries add load, load adds latency, latency adds retries. Server keeps working on abandoned requests (work amplification 1.4–1.7x).
- Async: 100% capture, ack p99 ~1.4ms at all rates.
- **Not proven:** durability (`acks=all`), ordering, Kafka-down behavior, dedup under redelivery, poison events, lag behavior. This build must prove those against a real broker.

## 3. Stack & layout
Java 21, Spring Boot 3.3, Maven multi-module (match my StreamForge conventions), Spring Kafka, Postgres, Micrometer -> Prometheus -> Grafana, Testcontainers, Toxiproxy, Docker Compose.
```
common/            event envelope, canonicalization, dedup-key, error taxonomy
ingest-service/    POST /callbacks -> validate -> produce -> ack
consumer-service/  consume -> idempotent process -> DLQ/retry
loadgen/           SFMC-style sender (3s timeout, retries, backoff, ledger of sent IDs)
infra/             compose (3-broker KRaft, Postgres, Toxiproxy, Prom, Grafana), dashboards, alert rules
docs/              ARCHITECTURE.md, EDGE_CASES.md, DEFENSE.md (interview Q&A mapped to experiments)
```
Keep a **sync baseline mode** in the ingest service (feature flag) so the same code and load can be compared sync vs async.

## 4. Design decisions (implement, then document tradeoffs)
**Ack contract:** 200 means "durably in Kafka", nothing more. Idempotent producer, `acks=all`, topic `replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`. Await the produce futures for the whole batch before responding; do not `flush()` per request. Validate (auth, size, envelope) *before* producing.
**Deadline budget:** the handler must finish well inside 3s (target <=2s hard deadline). Set `max.block.ms`, `request.timeout.ms`, `delivery.timeout.ms` consistently (delivery >= linger + request timeout) and below the deadline. Fail fast with 5xx rather than hang until SFMC times out.
**Partition key:** per-entity ordering key (subscriber/contact key), never batch id. Batches are split into per-entity events at produce time. Partition count is sized up front (increasing later breaks key->partition mapping); document why.
**Event identity:** dedup key = upstream event id if present, else SHA-256 of canonicalized event content. Envelope carries: event id, batch id, received-at, schema version, trace id, upstream event timestamp.
**Consumer:** at-least-once + idempotent sink. Business write and dedup record in one Postgres txn (`INSERT ... ON CONFLICT DO NOTHING` on unique event id); commit offsets only after the txn. Cooperative-sticky assignor.
**Retries/DLQ:** classify errors retryable vs non-retryable. Non-blocking retry topics with backoff for transient errors (blocking retries stall the partition and must be justified if used). Exhausted/non-retryable -> DLQ with headers (orig topic/partition/offset, exception class, attempt count, first-failure time). Provide a DLQ replay tool that re-enters through the idempotent path.
**State-transition guard:** status events can arrive out of order (SFMC retries, partition moves). Consumer applies a monotonic state machine / uses upstream timestamp so a late `sent` never overwrites `delivered`; legitimate transitions are never dropped.
**Backpressure:** bounded in-flight requests and bounded executor in ingest; shed with 503 (+Retry-After) instead of queueing until SFMC times out. Consumer pauses/resumes partitions when downstream is slow or rate-limited.

## 5. Edge cases — each needs code, a test, and an entry in docs/EDGE_CASES.md
**Ingest**
- Malformed JSON / schema-invalid envelope -> 400; oversize body -> 413 (configured limit, streamed check, no OOM); wrong content type -> 415.
- Auth: verify SFMC callback signature (HMAC over raw body, constant-time compare, key rotation support: accept current+previous) and the callback verification handshake. *Verify exact SFMC Event Notification header/handshake details from official docs before implementing; do not guess.* Optional replay window on timestamp.
- Empty batch -> 200 no-op. Batch with mixed valid/invalid events: do NOT reject the whole batch (a permanently bad event would cause an infinite retry loop of the batch). Ack the valid envelope, route invalid events to an `invalid-events` topic, count + alert.
- Duplicate batch from SFMC retry -> same event ids -> deduped downstream, never double-processed.
- Partial produce failure (some events in, some not) -> return 5xx so SFMC retries whole batch; safe because of idempotent consume. Test this explicitly.
- Kafka unavailable / metadata timeout / leader election / broker kill mid-request -> bounded wait then 503; circuit breaker around produce to fail fast; **never** ack-and-buffer in memory. Decide and document whether a local disk spool is in scope (default: no).
- Slow Kafka (latency injected) -> deadline respected, no thread pile-up (bulkhead).
- Graceful shutdown (SIGTERM): readiness flips false first, stop accepting, drain in-flight produces, close producer, then exit; no acked-but-unsent events.
- Large batch fan-out, hot subscriber key (partition skew), unknown extra fields (tolerate), unsupported schema version (route + alert), clock skew (don't trust upstream ts blindly).
- PII: no raw payloads in logs; redaction; structured logs with trace id.

**Consumer**
- Crash mid-batch / mid-txn -> redelivery -> exactly-once *effect* via idempotency (prove with kill -9 test).
- Rebalance during processing, duplicate delivery after rebalance, `max.poll.interval.ms` vs slow processing (cap per-poll work; no rebalance storms).
- Deserialization failure (`ErrorHandlingDeserializer`), poison message that never succeeds -> DLQ without blocking its partition.
- Downstream (DE API / DB) slow, 429, timeout, partial outage -> retry topics, backoff with jitter, pause/resume, rate limit.
- DB down / constraint races (two consumers same event) -> unique constraint is the arbiter.
- Out-of-order and late events (see state-transition guard); DLQ replay of old events must not regress state.
- Lag: alert on consumer lag *and* age of oldest unprocessed message; document drain-time budget.
- Retention: topic retention > worst-case outage + drain; DLQ retention longer.

**Measurement integrity (capture rate)**
- loadgen keeps a ledger of every event id it generated. After each run a verifier diffs ledger vs Postgres: `lost = sent - persisted`, `duplicates = persisted rows with same event id` (must be 0 by constraint), plus count of DLQ'd/invalid. Capture rate definitions (first-attempt 2xx per batch, and distinct-event persisted/expected) are both reported and clearly defined.

## 6. Build stages (stop at each gate, run everything, report results)
1. **Infra:** compose with 3-broker KRaft, Postgres, Toxiproxy (in front of Kafka for the services), Prometheus, Grafana. Gate: topics created with correct RF/min-ISR; kill one broker, produce with `acks=all` still succeeds.
2. **common + ingest-service (sync baseline + async mode):** gate: unit + integration tests (Testcontainers); handler deadline test; all ingest edge cases above covered.
3. **consumer-service:** idempotent sink, retry topics, DLQ, state guard. Gate: kill -9 test, poison test, rebalance test, out-of-order test.
4. **loadgen + verifier:** SFMC-style sender with 3s timeout/retries. Gate: reproduces the sync cliff on the real stack.
5. **Experiments (scripted, repeatable, results saved under docs/results/):** (a) sync vs async cliff (capture, ack p50/p99, work amplification); (b) broker kill mid-run: zero loss, zero downstream duplicates; (c) Toxiproxy latency/cut on Kafka link: deadline held, 5xx not hang; (d) poison event -> DLQ, partition unblocked; (e) per-subscriber ordering across rebalances; (f) lag build-up and drain time at 1.5x capacity.
6. **Observability + CI:** metrics (ack latency histogram, produce failures, in-flight, 4xx/5xx by cause, lag, oldest-message age, retry/DLQ rates, dedup hits), dashboards, alert rules, GitHub Actions running tests.
7. **Docs:** ARCHITECTURE.md, EDGE_CASES.md (case -> behavior -> test), DEFENSE.md.

## 7. DEFENSE.md must answer (with evidence links to results)
Why not scale the sync workers? What does 200 guarantee and what can still lose data? Kafka down at ack time? How is "capture rate" defined/measured? Partition key and ordering guarantee limits? How do duplicates and out-of-order events get handled? Poison event behavior? What happens when consumers fall behind? What would you change at 10x scale? Known limitations and what is *not* covered.

## 8. Working agreements
- Work in the stages above; run tests/builds yourself and report real output. Say plainly what you did not run.
- Small commits, one concern each. No giant code dumps in chat: summarize decisions and tradeoffs, point to files.
- Staff-level reasoning in docs: tradeoffs, failure modes, why-not-alternatives. Concise.
- If a requirement is ambiguous (e.g. SFMC signature details), research official docs or ask — don't invent.
- Prefer boring, well-understood configs over cleverness; every non-default Kafka/Spring setting gets a one-line rationale.
