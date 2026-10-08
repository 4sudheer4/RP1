package com.rp1.ingest.engine;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;

import java.time.Instant;
import java.util.List;

/** What happens to a validated batch: produce to Kafka (async) or process inline (sync baseline). */
public interface IngestPath {

    /** @param startNanos System.nanoTime() when the handler started (for latency/abandonment accounting) */
    record Batch(String batchId, String traceId, Instant receivedAt, long startNanos,
                 List<CallbackEnvelope> valid, List<EnvelopeFactory.Invalid> invalid) {}

    /** Outcome causes double as metric tag values; keep them stable. */
    enum Outcome {
        ACCEPTED(200),
        BREAKER_OPEN(503),
        DEADLINE_EXHAUSTED(503),
        PRODUCE_TIMEOUT(503),
        PRODUCE_FAILED(503),
        PROCESSING_FAILED(503);

        public final int status;
        Outcome(int status) { this.status = status; }
        public String tag() { return name().toLowerCase(); }
    }

    Outcome ingest(Batch batch, long deadlineNanos);
}
