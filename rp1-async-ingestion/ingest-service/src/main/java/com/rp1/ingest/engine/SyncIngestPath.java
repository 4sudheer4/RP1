package com.rp1.ingest.engine;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;
import com.rp1.sink.CallbackSink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Sync baseline, reproducing the old design on purpose: all per-event work (DB txn + downstream call) runs before
 * the response, with no deadline. When load exceeds downstream capacity, requests queue, exceed SFMC's timeout,
 * SFMC retries, and the server keeps working on abandoned requests. That last part is counted as
 * {@code rp1.ingest.sync.batches{late=true}}: work amplification.
 */
public class SyncIngestPath implements IngestPath {
    private static final Logger log = LoggerFactory.getLogger(SyncIngestPath.class);

    private final CallbackSink sink;
    private final long clientTimeoutNanos;
    private final Counter onTime, late;

    public SyncIngestPath(CallbackSink sink, Duration clientTimeout, MeterRegistry registry) {
        this.sink = sink;
        this.clientTimeoutNanos = clientTimeout.toNanos();
        this.onTime = Counter.builder("rp1.ingest.sync.batches").tag("late", "false").register(registry);
        this.late = Counter.builder("rp1.ingest.sync.batches").tag("late", "true").register(registry);
    }

    @Override
    public Outcome ingest(Batch batch, long deadlineNanosIgnored) {
        try {
            for (CallbackEnvelope e : batch.valid()) sink.process(e, CallbackSink.Source.SYNC);
            for (EnvelopeFactory.Invalid inv : batch.invalid()) {
                sink.recordInvalid(batch.batchId(), inv.reason().name(), batch.receivedAt(), String.valueOf(inv.event()));
            }
            return Outcome.ACCEPTED;
        } catch (RuntimeException e) {
            log.warn("sync processing failed: {}", e.toString());
            return Outcome.PROCESSING_FAILED;
        } finally {
            (System.nanoTime() - batch.startNanos() > clientTimeoutNanos ? late : onTime).increment();
        }
    }
}
