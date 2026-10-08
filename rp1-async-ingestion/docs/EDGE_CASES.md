# Edge cases: case → behavior → code → test

Test class shorthand: **CCT** = `ingest-service/.../api/CallbackControllerTest`, **KIP** = `.../engine/KafkaIngestPathTest`,
**SVT** = `SignatureVerifierTest`, **BRT** = `BodyReaderTest`, **IKIT** = `ingest-service/.../it/IngestKafkaIT`
(real Kafka behind Toxiproxy), **ISIT** = `IngestSyncModeIT`, **EFT** = `common/.../EnvelopeFactoryTest`,
**CSIT** = `sink/.../CallbackSinkIT` (real Postgres), **CIT** = `consumer-service/.../it/ConsumerIT`,
**CKIT** = `ConsumerKillIT` (kill -9 of the packaged jar). Experiments: `docs/results/stage5/<x>/`.

## Ingest

| Case | Behavior | Code | Test |
|---|---|---|---|
| Malformed JSON | 400 `malformed_json`, nothing produced | `CallbackController.handle` | CCT `malformedJsonAndNonArrayAre400` |
| Body not a JSON array | 400 `expected_json_array` (unless it's the ENS handshake) | same | CCT same |
| Oversize body | 413 `body_too_large`. Content-Length checked first, then a streamed cap (never buffers > limit + 8 KiB). Checked before auth, so it's cheap to reject | `BodyReader` | BRT (declared, endless stream, exact limit), CCT `oversizeBodyIs413BeforeAuth` |
| Too many events (> 1000) | 413 `too_many_events` | controller | CCT `tooManyEventsIs413` |
| Wrong content type | 415 (`application/json` with any params is accepted) | `isJson` | CCT `wrongContentTypeIs415`, `jsonWithCharsetIsAccepted` |
| Missing / bad signature | 401, nothing produced. HMAC-SHA256 over raw bytes, base64 header, `MessageDigest.isEqual`, all keys checked (no timing leak of which key matched) | `SignatureVerifier` | SVT, CCT `missingOrBadSignatureIs401AndNothingProduced` |
| Key rotation | current + previous key accepted; `rp1_ingest_signature_total{key="previous"}` shows when rotation is done | `SignatureVerifier`, alert `IngestSignatureKeyRotationIncomplete` | SVT `currentAndPreviousKeysAccepted`, CCT `previousKeyStillAcceptedDuringRotation` |
| Key encoding unknown (UTF-8 vs base64) | configurable, default UTF8; flagged as unverified | `KeyEncoding` | SVT `base64KeyEncodingDecodesKey` |
| ENS verification handshake | unsigned `{callbackId, verificationKey}` → 200 + details logged, only while enabled; wrong callbackId → 403; disabled → 401 | `handshake` | CCT `ensVerificationHandshake`, `handshakeRejectedWhenVerificationDisabled` |
| Replay window on timestamp | **Not implemented, deliberately.** The ENS signature doesn't cover a timestamp, so a replay window can't be authenticated. Replays are harmless anyway (dedup). | n/a | n/a |
| Empty batch | 200 `empty_batch`, no produce | controller | CCT `emptyBatchIs200NoOp` |
| Mixed valid/invalid events | 200; valid → `callback-events`, invalid → `invalid-events` with reason; counted (`rp1_ingest_events_total{result="invalid",reason}`) and alerted. Never reject the batch (a permanently bad event would make SFMC retry, then drop, the good ones) | `EnvelopeFactory`, `KafkaIngestPath.invalidRecord` | CCT `mixedBatchAccepts...`, IKIT `acceptedBatchIsDurablyInKafka...`, EFT `invalidReasons`, exp (d) |
| Duplicate batch (SFMC retry) | same bytes → same event ids (batch id differs) → deduped downstream | `Canonical.eventId` | CCT `duplicateBatchProducesSameEventIds`, ISIT, CIT `duplicatesAndOutOfOrder...` |
| Partial produce failure | 503 (SFMC retries the whole batch; already-written events dedup) | `KafkaIngestPath` | KIP `partialProduceFailureIsNotAccepted` |
| Kafka down / metadata timeout / `send()` throws | bounded: `max.block.ms`, the deadline re-checked between sends, `allOf.get(remaining)` → 503 | `KafkaIngestPath` | KIP `synchronousSendException...`, `noBudgetLeftMeansNoProduce`, IKIT `kafkaCutReturns503WithinDeadlineNeverHangs`, exp (c) |
| Broker killed / leader election mid-request | either acked (200) or 503 within the deadline; SFMC retry recovers; no loss | producer config + deadline | exp (b) |
| Slow Kafka (latency) | deadline held, breaker opens → instant 503s, recovers when healthy | breaker, deadline | IKIT `slowKafkaDeadlineHeldNoPileUp...`, exp (c) |
| Thread pile-up / overload | bulkhead (64) → instant 503 + Retry-After; small Tomcat accept queue | controller semaphore | CCT `bulkheadShedsWith503InsteadOfQueueing`, IKIT burst of 80 |
| Never ack-and-buffer | no code path returns 200 before all futures complete; no in-memory or disk spool | `KafkaIngestPath` | KIP `acceptedOnlyAfterEveryRecordAcked...`, `neverAckedReturnsTimeoutWithinDeadline` |
| Graceful shutdown (SIGTERM) | readiness → REFUSING_TRAFFIC; Tomcat graceful drain (in-flight requests finish, ≤ deadline); producer closed last | `IngestApplication.onClose`, `server.shutdown=graceful` | IKIT `gracefulShutdownDrainsInFlightRequestAndItsEventIsInKafka` |
| Cold start | readiness DOWN until producer metadata fetched | `ProducerReadiness` | IKIT (each test waits for readiness) |
| Large batch fan-out | up to 1000 records produced concurrently and awaited together | `KafkaIngestPath` | IKIT, exp (a) (20-event batches at 120 b/s) |
| Hot subscriber key | all its events go to one partition (ordering); throughput for that key is capped at one thread | partition key | exp (e) `--hot-fraction 0.05` |
| Unknown extra fields | tolerated and preserved in the payload | `Json.MAPPER` (`FAIL_ON_UNKNOWN_PROPERTIES` off) | EFT `unknownExtraFieldsTolerated` |
| Unsupported event type / schema | routed to `invalid-events` as `UNSUPPORTED_EVENT_TYPE`, alerted; envelope `schemaVersion` > supported → consumer DLT | `EnvelopeFactory`, `CallbackListener` | EFT `invalidReasons`, CCT mixed batch |
| Clock skew | `timestampUTC` beyond `receivedAt + 5m` is clamped for ordering, flagged and counted; raw value kept | `EnvelopeFactory` | EFT `futureTimestampBeyondSkewIsClampedAndFlagged` |
| PII in logs | no payloads logged; subscriber keys hashed; Spring's key-logging producer listener replaced; JSON logs with traceId | controller, `IngestConfig.redactingProducerListener`, `logback-spring.xml` | code review; the IT logs show no payloads |

## Consumer

| Case | Behavior | Code | Test |
|---|---|---|---|
| Crash mid-batch / mid-txn (kill -9) | uncommitted txn rolls back; offsets weren't committed → redelivery; the PK absorbs what was already committed | `CallbackSink`, `ack-mode: batch` | CKIT (2000 events, killed at ~400 → exactly 2000 rows, all final states right); exp (e) |
| Rebalance during processing | cooperative-sticky; records redelivered to the new owner are deduped | assignor config | CIT `rebalanceMidStream...`, exp (e) |
| `max.poll.interval.ms` vs slow processing | `max.poll.records` 50, bounded downstream waits → per-poll work ≪ 300s | config | by construction (ARCHITECTURE §7); exp (f) runs at saturation |
| Deserialization failure | `ErrorHandlingDeserializer` → DLT with the original bytes, partition continues | config, `EnvelopeDeserializer` | CIT `poisonAndUndeserializable...` |
| Poison (never succeeds) | non-retryable → DLT immediately; retryable-forever → 3 retry topics → DLT; the partition is never blocked | `RetryTopicConfig` | CIT poison + `transientFailureRetriesThroughRetryTopicsThenDlt`, exp (d) |
| Downstream slow / 429 / timeout / outage | retryable → retry topics with jittered exponential backoff; 429 additionally pauses the container; optional token bucket | `ThrottleGuard`, `RateLimiter`, `SimulatedDownstream` | CIT flaky; exp (f) runs the downstream at saturation |
| DB down | `DataAccessException` → retryable → retry topics; Hikari `connection-timeout` 3s keeps it bounded | `CallbackSink.process` | CSIT `downstreamFailureRollsBack...` (rollback path); DB outage not run as an experiment (see DEFENSE "not covered") |
| Two consumers, same event | the PK serializes them; exactly one APPLIED, one DUPLICATE | `CallbackSink` | CSIT `concurrentConsumersSameEventExactlyOneApplies` |
| Out-of-order / late events | status only moves forward in (rank, ordering_ts, event_id); late events are still recorded in the log | `UPSERT_STATUS` guard | CSIT `lateSentNeverOverwritesBounced`, `finalStateIsIndependentOfArrivalOrder`; CIT shuffled; verifier `statusMismatches` in every experiment |
| DLQ replay of old events | idempotent (no duplicate rows), can't regress status | `DlqReplay` + sink | exp (d) |
| Lag | alerts on records **and** oldest-unprocessed age | `LagAgeMonitor`, alerts | exp (f) |
| Retention | main 7d, retry 7d, DLT 14d, invalid 14d | `create-topics.sh` | Stage 1 gate checks configs |

## Measurement integrity

| Case | Behavior | Code |
|---|---|---|
| Ledger | every generated event id (same function as ingest) with batch, validity, message, type | `Sender`, `LoadgenTest` |
| Lost | valid ledger ids missing from Postgres after drain, minus those found in the DLT | `Verifier` |
| Duplicates | ids with >1 row (0 by the PK; reported anyway) | `Verifier` |
| Two capture rates | batch-level (2xx on 1st try / within 3 retries) and event-level (distinct persisted / generated) | `Sender`, `Verifier` |
| Status correctness | messages whose status events all persisted must have the lifecycle's final status | `Verifier.statusCheck` |
