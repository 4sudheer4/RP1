# Stage 1 — Results (exact, from logs)

Raw logs: `docs/results/stage1/20261007T171834/gate.log` (run 1) and `docs/results/stage1/20261007T172201/gate.log` (run 2).
Both runs started from an empty cluster (`--fresh`). Machine: macOS laptop, Docker Desktop 27.5.1 (8 CPUs, ~8 GB to Docker).
Latency is measured from `send()` to the ack callback.

## Scorecard

| Check | Run 1 | Run 2 |
|---|---|---|
| callback-events: 12p RF3 minISR2 unclean=false, all ISR=3 | PASS | PASS |
| callback-events-dlt: 3p RF3 minISR2 unclean=false, all ISR=3 | PASS | PASS |
| invalid-events: 3p RF3 minISR2 unclean=false, all ISR=3 | PASS | PASS |
| stage1-gate: 12p RF3 minISR2 unclean=false, all ISR=3 | PASS | PASS |
| Broker defaults on all 3 brokers | PASS | PASS |
| Baseline: all acked | PASS | PASS |
| 300ms latency toxic shows up in ack latency | PASS | PASS |
| All proxies disabled → every send fails | PASS | PASS |
| Broker killed mid-run: every `acks=all` send still acked | PASS | PASS |
| kafka-2 fenced: leads nothing, out of every ISR | PASS | PASS |
| Degraded cluster (2/3) still accepts `acks=all` | PASS | PASS |
| `acks=all` rejected with NotEnoughReplicas when ISR < min ISR | PASS | PASS |
| 1 replica alive → nothing acked after the kill | **FAIL** (script bug) | PASS |
| Cold producer: nothing acked, app deadline bounds the wait | PASS | PASS |
| All gate partitions back to ISR=3 | PASS | PASS |
| Zero loss: every acked ID is in the log | PASS | PASS |
| **Total** | **15 / 16** | **16 / 16** |

## Per-phase numbers

| Phase | Run 1 | Run 2 |
|---|---|---|
| A baseline (1000) | 1000 acked, 0 failed; p50 271.6ms, p99 285.5ms, max 548.4ms | 1000 acked, 0 failed; p50 287.8ms, p99 299.4ms, max 519.3ms |
| B 300ms latency toxic (50) | 50 acked; p50 635.8ms, p99 1496.0ms | 50 acked; p50 640.1ms, p99 1486.9ms |
| C proxies disabled (5) | 0 acked, 5 failed, each after ~3.0s (`max.block.ms`=3000) | 0 acked, 5 failed, each after ~3.0s |
| D kill kafka-2 mid-run (6000 @ 600/s) | **6000 acked, 0 failed**; p50 100.9ms, **p99 9520.8ms**, max 9702.5ms | **6000 acked, 0 failed**; p50 188.4ms, **p99 8503.4ms**, max 8661.9ms |
| E degraded, 2 of 3 brokers (1000) | 1000 acked; p50 22.8ms, p99 42.4ms | 1000 acked; p50 131.4ms, p99 142.7ms |
| F min ISR 3, one broker down (20) | 0 acked, 20 × `NotEnoughReplicasException` | 0 acked, 20 × `NotEnoughReplicasException` |
| G kill kafka-3, warm producer (2000 @ 100/s) | 613 acked, 1387 failed (all `TimeoutException: Expiring N record(s) … 10000 ms`) | 690 acked, 1310 failed (same error); **last ack 100ms before kafka-3 died** |
| H cold producer, quorum lost (20) | 0 acked, 20 failed, ~10s each, run 13964ms | 0 acked, 20 failed, ~10s each, run 13615ms |
| Recovery to full ISR | ~2s after brokers healthy | ~2s after brokers healthy |
| Verify | acked 8663, failed 1412; log distinct 8846; **LOSS 0**; duplicate IDs 0; failed-but-present **183** | acked 8740, failed 1335; log distinct 9065; **LOSS 0**; duplicate IDs 0; failed-but-present **325** |

The baseline p50 of ~270–290ms is inflated by the producer starting cold (metadata fetch, producer ID, connections)
inside a short 1000-message burst. Phase E, run on an already-running cluster, shows normal ack times of tens of ms.
Stage 2 will measure steady-state latency properly.

## Incidents during testing

### 1. Gate script bug: wrong kill timestamp (run 1, step 7 FAIL)

- The check said the last ack came 3182ms after the kill, which would mean acks with only one replica alive.
- Cause: the script took its timestamp *before* calling `docker compose kill`, and each `docker compose` command
  takes about **3.4s** to start on this machine. Docker's own record showed kafka-3 actually died at
  epoch-ms 1791418843231, while the last ack was at 1791418843180, **51ms before** the broker died.
- Fix: the script now reads the kill time from Docker's recorded exit time (`State.FinishedAt`).
  Run 2 passed with the fix.

### 2. Hung run: fresh producer + lost quorum (first attempt, aborted, log not kept)

- The first version of step 7 started a *new* producer after killing kafka-3. It never finished, despite
  `delivery.timeout.ms=10000`.
- Thread dump: the producer's network thread was stuck in `Sender.maybeSendAndPollTransactionalRequest`. An
  idempotent producer needs a producer ID before it can send, the cluster can't hand one out without a quorum, and
  messages waiting for that ID are never expired.
- Fix in the gate tool: an app-side `--deadline-ms`; past it, the producer is force-closed and the remaining sends
  count as failed. Step 7 was split into 7 (producer already running when the broker dies, the realistic case) and
  7b (fresh producer, bounded by the app deadline).
- This is a real finding, not just a test-harness issue. See [stage1-findings.md](stage1-findings.md).

### 3. Laptop went to sleep (run 2, during step 7)

- macOS power log: `17:23:56 Entering Sleep state due to 'Idle Sleep' … Using Batt (Charge:15%)`, woke at 17:27:03.
- Step 7's producer started at 17:23:56; the kill ran at 17:27:05, after wake.
- Why the check still holds: it compares the producer's own wall-clock ack times with Docker's recorded exit time
  of kafka-3, and both were taken after wake. The producer's send pacing paused during sleep and resumed, so it was
  still sending when kafka-3 died: 690 acked before, 1310 failed after, last ack 100ms before death.
- Run 1 had an unexplained ~11-minute pause at the same point, almost certainly the same cause.
- Fix: the gate script now re-runs itself under `caffeinate -i` on macOS so the laptop can't idle-sleep mid-run.
  This fix hasn't been through a full run yet.

## What was verified vs. not

Verified on this stack:
- Topic and broker durability settings.
- `acks=all` survives a single broker crash (SIGKILL) mid-stream with zero loss.
- Min ISR is enforced by the broker.
- With 2 of 3 brokers dead, nothing gets confirmed.
- Recovery back to full replication.
- Client traffic goes through Toxiproxy.

Not verified yet:
- Graceful broker shutdown (only `SIGKILL` was tested).
- Network partition, as opposed to a crash.
- Disk-full and slow-disk failures.
- Behaviour under sustained high throughput.
- Anything about the services. Those are Stages 2–5.
