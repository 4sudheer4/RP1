package com.rp1.consumer.engine;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.Canonical;
import com.rp1.common.Topics;
import com.rp1.common.errors.NonRetryableException;
import com.rp1.common.errors.ThrottledException;
import com.rp1.sink.CallbackSink;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.time.Instant;

/**
 * At-least-once consumer over the idempotent sink. Offsets are committed by the container only after this
 * method returns (after the Postgres txn committed); a crash anywhere before that redelivers the record, and the
 * sink's primary key turns the redelivery into a no-op. The same method serves the retry topics.
 */
public class CallbackListener {
    public static final String MAIN_ID = "callbacks";
    private static final Logger log = LoggerFactory.getLogger(CallbackListener.class);

    private final CallbackSink sink;
    private final ThrottleGuard throttle;
    private final @Nullable RateLimiter limiter;
    private final MeterRegistry registry;

    public CallbackListener(CallbackSink sink, ThrottleGuard throttle, @Nullable RateLimiter limiter, MeterRegistry registry) {
        this.sink = sink;
        this.throttle = throttle;
        this.limiter = limiter;
        this.registry = registry;
    }

    @KafkaListener(id = MAIN_ID, topics = Topics.CALLBACK_EVENTS, concurrency = "${rp1.consumer.concurrency}")
    public void onRecord(ConsumerRecord<String, CallbackEnvelope> record) {
        CallbackEnvelope e = record.value();
        MDC.put("traceId", e.traceId());
        try {
            if (e.schemaVersion() > CallbackEnvelope.CURRENT_SCHEMA) {
                throw new NonRetryableException("unsupported envelope schema version " + e.schemaVersion());
            }
            if (limiter != null) limiter.acquire();
            CallbackSink.Outcome outcome = sink.process(e, CallbackSink.Source.kafka(record.partition(), record.offset()));
            throttle.onSuccess();
            String stage = record.topic().equals(Topics.CALLBACK_EVENTS) ? "main" : "retry";
            Timer.builder("rp1.consumer.e2e.latency").description("ingest receivedAt -> processed")
                    .tag("stage", stage).publishPercentileHistogram().register(registry)
                    .record(Duration.between(e.receivedAt(), Instant.now()));
            if (outcome == CallbackSink.Outcome.DUPLICATE) {
                log.debug("duplicate delivery absorbed eventId={} partition={} offset={}", e.eventId(), record.partition(), record.offset());
            }
        } catch (ThrottledException t) {
            throttle.onThrottled();
            throw t;
        } catch (RuntimeException ex) {
            log.warn("processing failed eventId={} subscriber={} topic={} error={}", e.eventId(),
                    Canonical.redact(e.subscriberKey()), record.topic(), ex.toString());
            throw ex;
        } finally {
            MDC.remove("traceId");
        }
    }
}
