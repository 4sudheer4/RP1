package com.rp1.ingest.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;
import com.rp1.common.Headers;
import com.rp1.common.Json;
import com.rp1.common.Topics;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Async path. The ack contract: return ACCEPTED only after every record of the batch (valid events to
 * callback-events, invalid ones to invalid-events) is acknowledged by Kafka with acks=all. Nothing is buffered
 * in memory past the response: a 503 means "not confirmed", and SFMC's retry plus consumer dedup handles the
 * case where some records did land (Stage 1 finding #1: a failed send can still end up in the log).
 *
 * <p>The wait is bounded by the request deadline, not only by Kafka client timeouts: an idempotent producer can
 * wait indefinitely for a producer id while the controller quorum is down (Stage 1 finding #2).
 */
public class KafkaIngestPath implements IngestPath {
    private static final Logger log = LoggerFactory.getLogger(KafkaIngestPath.class);

    private final KafkaTemplate<String, byte[]> kafka;
    private final CircuitBreaker breaker;
    private final long minBudgetNanos;
    private final Counter produceTimeouts, produceFailures;

    public KafkaIngestPath(KafkaTemplate<String, byte[]> kafka, CircuitBreaker breaker, long minBudgetNanos, MeterRegistry registry) {
        this.kafka = kafka;
        this.breaker = breaker;
        this.minBudgetNanos = minBudgetNanos;
        this.produceTimeouts = Counter.builder("rp1.ingest.produce.failures").tag("cause", "timeout").register(registry);
        this.produceFailures = Counter.builder("rp1.ingest.produce.failures").tag("cause", "error").register(registry);
    }

    @Override
    public Outcome ingest(Batch batch, long deadlineNanos) {
        if (!breaker.tryAcquirePermission()) return Outcome.BREAKER_OPEN;
        long start = System.nanoTime();
        List<CompletableFuture<SendResult<String, byte[]>>> futures = new ArrayList<>(batch.valid().size() + batch.invalid().size());
        try {
            for (CallbackEnvelope e : batch.valid()) {
                // send() can block up to max.block.ms (missing metadata, full buffer), so re-check the budget each time.
                if (deadlineNanos - System.nanoTime() < minBudgetNanos) return fail(start, Outcome.DEADLINE_EXHAUSTED, null);
                futures.add(kafka.send(record(e)));
            }
            for (EnvelopeFactory.Invalid inv : batch.invalid()) {
                if (deadlineNanos - System.nanoTime() < minBudgetNanos) return fail(start, Outcome.DEADLINE_EXHAUSTED, null);
                futures.add(kafka.send(invalidRecord(batch, inv)));
            }
            long remaining = deadlineNanos - System.nanoTime();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
            breaker.onSuccess(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            return Outcome.ACCEPTED;
        } catch (TimeoutException e) {
            produceTimeouts.increment();
            return fail(start, Outcome.PRODUCE_TIMEOUT, e);
        } catch (ExecutionException e) {
            produceFailures.increment();
            return fail(start, Outcome.PRODUCE_FAILED, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return fail(start, Outcome.PRODUCE_FAILED, e);
        } catch (RuntimeException e) {
            // KafkaTemplate.send can throw synchronously (e.g. max.block.ms expiry, producer closed)
            produceFailures.increment();
            return fail(start, Outcome.PRODUCE_FAILED, e);
        }
    }

    private Outcome fail(long start, Outcome outcome, Throwable cause) {
        breaker.onError(System.nanoTime() - start, TimeUnit.NANOSECONDS,
                cause != null ? cause : new TimeoutException(outcome.tag()));
        if (cause != null) log.warn("produce not confirmed: outcome={} cause={}", outcome.tag(), cause.toString());
        return outcome;
    }

    static ProducerRecord<String, byte[]> record(CallbackEnvelope e) {
        ProducerRecord<String, byte[]> r = new ProducerRecord<>(Topics.CALLBACK_EVENTS, e.subscriberKey(), toBytes(e));
        r.headers().add(new RecordHeader(Headers.EVENT_ID, utf8(e.eventId())));
        r.headers().add(new RecordHeader(Headers.BATCH_ID, utf8(e.batchId())));
        r.headers().add(new RecordHeader(Headers.TRACE_ID, utf8(e.traceId())));
        r.headers().add(new RecordHeader(Headers.SCHEMA_VERSION, utf8(Integer.toString(e.schemaVersion()))));
        return r;
    }

    static ProducerRecord<String, byte[]> invalidRecord(Batch batch, EnvelopeFactory.Invalid inv) {
        ObjectNode v = Json.MAPPER.createObjectNode();
        v.put("reason", inv.reason().name());
        v.put("batchId", batch.batchId());
        v.put("traceId", batch.traceId());
        v.put("receivedAt", batch.receivedAt().toString());
        v.set("event", inv.event());
        ProducerRecord<String, byte[]> r = new ProducerRecord<>(Topics.INVALID_EVENTS, batch.batchId(), v.toString().getBytes(StandardCharsets.UTF_8));
        r.headers().add(new RecordHeader(Headers.TRACE_ID, utf8(batch.traceId())));
        return r;
    }

    private static byte[] toBytes(CallbackEnvelope e) {
        try {
            return Json.MAPPER.writeValueAsBytes(e);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static byte[] utf8(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }
}
