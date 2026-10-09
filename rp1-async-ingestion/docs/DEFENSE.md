# Defense: interview Q&A mapped to evidence

Every number below comes from a run in `docs/results/` on one laptop (Apple Silicon, 16 GB; Docker VM 7.65 GB
shared with other stacks). **Measured here** ≠ **measured in production**. The resume's 70% / 99.9% are not
reproduced or claimed by this build.

Main evidence: the sync vs async sweep, [a-cliff/20261008T153227/results.md](results/stage5/a-cliff/20261008T153227/results.md).
Setup: open-loop SFMC-style sender, 20 events/batch, 40s per rate, 2s client timeout, 3 retries (1s/2s/4s).
Both modes share the same downstream capacity (8 concurrent × 5ms per event) and the same business code (`sink`).

| rate (batches/s) | sync 1st-try 2xx | sync 2xx within 3 retries | sync events persisted | async 1st-try 2xx | async 2xx within retries | async events persisted | async ack p50 / p99 |
|---|---|---|---|---|---|---|---|
| 40 | 100% | 100% | 100% | 100% | 100% | 100% | 14 / 393 ms |
| 60 | 99.96% | 100% | 100% | 100% | 100% | 100% | 16 / 419 ms |
| 70 | **16.0%** | 18.9% | 100% | 100% | 100% | 100% | 23 / 556 ms |
| 80 | 1.5% | 1.6% | 100% | 85.2%* | 100% | 100% | 19 / 1052 ms* |
| 90 | 3.3% | 3.5% | 98.3% | 100% | 100% | 100% | 24 / 364 ms |
| 100 | 1.3% | 1.3% | 95.2% | rerun pending | | | |
| 120 | 0.8% | 0.8% | 80.1% | rerun pending | | | |

\* The breaker incident, explained under "Kafka down at ack time?" (since fixed). Async 100/120 overlapped laptop sleep and were discarded.

---

### Why not just scale the sync workers?

Because our threads were never the limit. The downstream (Data Extension API) was. In the sync sweep the server
had 200 Tomcat threads and 60 DB connections, but the downstream allows 8 concurrent calls, and that put the
knee between 60 and 70 batches/s. Above it, the server isn't idle: it processed **3.46× (70 b/s) to 3.95× (80 b/s)
as many batches as SFMC generated**, and 28–48% of that work finished after SFMC had already timed out. Retries
add load, load adds latency, latency causes retries. That's why it's a cliff (100% → 16% between 60 and 70)
rather than a slope. More workers would raise the in-flight count against the same 8 downstream slots, which
lengthens queues and makes it worse. Async removes the coupling: the 2s SFMC budget now covers only "is it in Kafka",
which took **p50 14–24 ms** at every rate measured, and the downstream works at its own pace.

### What does a 200 guarantee, and what can still lose data?

200 = every event of the batch acknowledged by Kafka with `acks=all`, RF3, `min.insync.replicas=2`
([ARCHITECTURE §3](ARCHITECTURE.md#3-the-ack-contract)). Evidence that acked writes survive one broker loss:
Stage 1 gate, 8,740 acked, **0 missing** after killing brokers mid-stream ([stage1 gate.log](results/stage1/20261007T172201/gate.log)).
What can still lose it: two of three replicas lost before flush, or someone enabling unclean leader election.
What a 200 does *not* mean: processed. Processing is the consumer's job and is checked by the verifier.
Also, **a 503 doesn't mean "not written"**: Stage 1 found 325 sends reported failed that were in the log. The
retry duplicates them, and the primary key removes the duplicate.

### Kafka down at ack time?

Bounded and fast. Produce futures are awaited with the remaining budget of a 1.5s deadline, and `send()` blocking
is capped by `max.block.ms`. The app-level wait matters because Stage 1 showed an idempotent producer can wait
indefinitely for a producer id during quorum loss. A circuit breaker turns a sustained outage into ~0 ms 503s.
Tests: link cut → 503 inside the deadline; 3s latency → 503 inside the deadline, breaker rejections <300 ms, recovery
after removal (`IngestKafkaIT`). Nothing is buffered: SFMC keeps the data and retries.
**A lesson from the sweep:** the first breaker config counted *slow successful* calls (>1s) as failures. One ~1s
Kafka stall at 80 b/s opened it, and ~1,100 requests got needless 503s in 5s (SFMC retries recovered all of
them). With SFMC's suspension rule, that is exactly what gets a callback suspended. The breaker now opens only on
calls that failed or missed the deadline, and stays open 2s. Likewise the bulkhead was 64, which shed 0.6% of
requests at 100 b/s during normal p99 bursts. Under the ENS suspension model that cost 25% of batches
([ens-bulkhead64](results/stage5/a-cliff/20261008T144236-ens-bulkhead64/results.md)), so it's now sized by
Little's law (180). The general point: **with SFMC, a 503 is not free.** Shed only when you can't succeed.

### How is "capture rate" defined and measured?

Two definitions, both reported, never conflated:
1. **Batch capture** = share of batches that got a 2xx, on the first attempt or within ENS's 3 retries. That's what SFMC sees.
2. **Event capture** = distinct valid generated event ids present in Postgres after drain / generated. That's what the business sees.

The loadgen writes a ledger of every event id, computed with the same function ingest uses, and the verifier
diffs it against Postgres and the DLT ([Verifier](../loadgen/src/main/java/com/rp1/loadgen/Verifier.java)).
They diverge, and that's informative. Sync at 80 b/s: batch capture 1.6%, but event capture 100%, because the
server eventually processed the abandoned requests. From SFMC's side, though, 98% of deliveries failed, and per the
official docs that triggers **callback suspension, during which events are dropped at the source**. With
suspension modeled (time-scaled), sync at 70 b/s persisted only **45%** of events
(30,800 lost; 1,540 batches dropped while suspended) and 14% at 100 b/s, while async at 40/70 b/s stayed at 100%
([ens-bulkhead64](results/stage5/a-cliff/20261008T144236-ens-bulkhead64/results.md); that run used the old bulkhead,
and its async-100 row is the bulkhead finding above). Without suspension, sync lost events outright from 90 b/s
(1,240 at 90, 19,100 at 120). Those batches never reached the server: TCP connects failed while it was saturated
(accept backlog), which is our interpretation from server logs showing no trace of them.

### Partition key and the limits of the ordering guarantee?

Key = `subscriberKey`, so one subscriber's events stay in partition order. 12 partitions, fixed up front because
increasing them remaps keys. The limits: SFMC retries, our retry topics and DLT replay all reorder. So correctness
doesn't rely on order. The status projection only moves forward in (rank, ordering_ts, event_id), which makes the
final state independent of arrival order. Evidence: every verifier run checks final status for all fully-persisted
messages, with **0 mismatches** in every run above (e.g. 2,672 messages in the smoke run). There's also an IT with
every event delivered twice, shuffled, and 10 shuffled orderings in `CallbackSinkIT`. A hot subscriber is limited to
one consumer thread; rebalance plus hot-key experiment (e) is pending a re-run.

### How are duplicates and out-of-order events handled?

Duplicates: the dedup key is the event's content hash (ENS has no event id). One Postgres transaction does the
`INSERT … ON CONFLICT (event_id) DO NOTHING` + downstream call + guarded status upsert. Offsets commit after the
transaction. **Duplicate rows: 0 in every run** (by construction, PK, and reported anyway). Crash proof:
`kill -9` of the consumer jar mid-stream, 2000 events, killed at ~405 rows → **exactly 2000 rows, 1000/1000 final states
correct**, in three separate runs ([stage3](results/stage3/)). Two consumers racing on one event serialize on the PK.
Out of order: a late `sent` after `bounced` is recorded in the log but doesn't change status (IT).
Limit: the downstream side effect is at-least-once (a crash between call and commit repeats it). Measured repeats were 0 in
3 kill runs, but the window exists, so the downstream must be idempotent by `eventId`.

### Poison event behavior?

Non-retryable (downstream 4xx, constraint violation, unsupported schema, bytes that don't deserialize) → DLT
immediately. Transient → `retry-0/1/2` (1s/2s/4s, jittered) → DLT. Non-blocking: the partition keeps moving.
IT evidence: poison + corrupt bytes ahead of 20 good events *on the same partition*; the good events are processed,
both bad records land in the DLT with `kafka_original-*`, `rp1-attempts=1`, `rp1-error-class=non_retryable`; a
flaky event goes through all three retry topics, `rp1-attempts=4`. Replay: `loadgen dlq-replay`, idempotent
and non-regressing. Experiment (d) at load is pending a re-run.

### What happens when consumers fall behind?

Ingest is unaffected: async at 90 b/s (above the ~80 b/s consumer capacity) still acked 100% first try with p50 24 ms,
and every event was persisted after drain. Kafka is the buffer; retention is 7 days, so drain time
≈ backlog / (capacity − arrival). We alert on lag **and** on the age of the oldest unprocessed record (read at the
committed offset), because records alone don't tell you minutes. Experiment (f) (1.5× capacity, lag
peak, drain time) is pending a re-run.

### What would you change at 10x?

- **Ingest:** stateless; scale horizontally behind the LB. Measure produce p99 vs the 1.5s deadline per instance and
  size the bulkhead by Little's law. Consider SFMC `maxBatchSize` 1000 to amortize request overhead.
- **Kafka:** 12 partitions cap consumer parallelism at 12. At 10x pick the count for the 2-3 year horizon now
  (e.g. 48–96) since increasing it later breaks key ordering. Split controllers from brokers (the Stage 1b brief),
  rack-aware replicas, quotas.
- **Consumer:** the bottleneck is the downstream. Batch writes per partition (one transaction per N events), use
  the DE API's bulk endpoints, and set a client-side rate limit to its published quota instead of discovering it via 429s.
- **Hot keys:** a single subscriber can't go faster than one thread. If that matters, per-key ordering only
  needs to hold per *message*, so key by messageKey and keep the status guard.
- **Postgres:** partition `callback_event` by time and expire it; the dedup window only needs to cover SFMC's retry
  window plus replay horizon.

### Known limitations / not covered

- **Not verified against a real SFMC account:** the signature key encoding (UTF-8 vs base64; configurable), the
  ENS retry backoff (assumed 1s/2s/4s), and the exact meaning of "consecutive delivery failures" for suspension
  (modeled per attempt, time-scaled 60x, without the escalation to 1h).
- **Single laptop:** 3 brokers on one Docker VM share one disk and CPU with Postgres and the services. Absolute
  numbers (capacity knee ~65 b/s, p99s) are this machine's, not production's. The *shape* (cliff vs flat) is the claim.
- **Simulated downstream:** fixed capacity + service time. Real DE API latency distributions, 429 semantics and
  partial outages were not reproduced.
- **Pending re-runs** (lid closed / laptop asleep mid-run on 2026-10-08, those runs were discarded):
  async 100/120 b/s, the ENS-suspension sweep with the fixed config, and experiments (b) broker kill,
  (c) Toxiproxy, (d) poison at load, (e) rebalance + kill -9 under load, (f) lag/drain. Scripts are ready:
  `infra/experiments/run-remaining.sh`. Until then, (b)–(e) are covered by ITs and the Stage 1 gate, not by load runs.
- **Not done:** DB outage experiment, TLS/SASL/ACLs (Stage 1b), multi-AZ, real disks, sustained hours-long soak.
