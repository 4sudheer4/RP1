package com.rp1.ingest.engine;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;
import com.rp1.common.Json;
import com.rp1.common.Topics;
import com.rp1.ingest.TestBatches;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.mock.MockProducerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaIngestPathTest {
    static final long DEADLINE = TimeUnit.MILLISECONDS.toNanos(400);

    MockProducer<String, byte[]> producer;
    CircuitBreaker breaker;
    KafkaIngestPath path;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        breaker = CircuitBreaker.of("t", CircuitBreakerConfig.custom()
                .slidingWindowSize(4).minimumNumberOfCalls(4).failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30)).build());
        var template = new KafkaTemplate<>(new MockProducerFactory<>(() -> producer));
        path = new KafkaIngestPath(template, breaker, TimeUnit.MILLISECONDS.toNanos(20), new SimpleMeterRegistry());
    }

    private IngestPath.Batch batch(int valid, int invalid) throws Exception {
        var f = new EnvelopeFactory(Duration.ofMinutes(5));
        List<CallbackEnvelope> v = new ArrayList<>();
        for (int i = 0; i < valid; i++) {
            v.add(((EnvelopeFactory.Valid) f.build(Json.MAPPER.readTree(TestBatches.event("EmailSent", "sub-" + i, "m-" + i, i)),
                    "b1", Instant.now(), "t1")).envelope());
        }
        List<EnvelopeFactory.Invalid> inv = new ArrayList<>();
        for (int i = 0; i < invalid; i++) {
            inv.add(new EnvelopeFactory.Invalid(EnvelopeFactory.InvalidReason.UNSUPPORTED_EVENT_TYPE, Json.MAPPER.readTree("{\"x\":" + i + "}")));
        }
        return new IngestPath.Batch("b1", "t1", Instant.now(), System.nanoTime(), v, inv);
    }

    /** Completes (or fails) pending sends from another thread, like the producer's I/O thread would. */
    private void completeAsync(int successes, boolean failOne) {
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(30);
                for (int i = 0; i < successes; i++) while (!producer.completeNext()) Thread.sleep(1);
                if (failOne) while (!producer.errorNext(new NotEnoughReplicasException("isr < min"))) Thread.sleep(1);
            } catch (InterruptedException ignored) {
            }
        });
    }

    @Test
    void acceptedOnlyAfterEveryRecordAcked_keyedBySubscriber_invalidRouted() throws Exception {
        completeAsync(4, false);
        var outcome = path.ingest(batch(3, 1), System.nanoTime() + DEADLINE);
        assertThat(outcome).isEqualTo(IngestPath.Outcome.ACCEPTED);
        List<ProducerRecord<String, byte[]>> sent = producer.history();
        assertThat(sent).hasSize(4);
        assertThat(sent.subList(0, 3)).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(Topics.CALLBACK_EVENTS);
            assertThat(r.key()).startsWith("sub-");
            assertThat(r.headers().lastHeader("rp1-event-id")).isNotNull();
        });
        assertThat(sent.get(3).topic()).isEqualTo(Topics.INVALID_EVENTS);
    }

    @Test
    void partialProduceFailureIsNotAccepted() throws Exception {
        completeAsync(2, true); // 2 of 3 land, 1 fails
        var outcome = path.ingest(batch(3, 0), System.nanoTime() + DEADLINE);
        assertThat(outcome).isEqualTo(IngestPath.Outcome.PRODUCE_FAILED);
    }

    @Test
    void neverAckedReturnsTimeoutWithinDeadline() throws Exception {
        long t0 = System.nanoTime();
        var outcome = path.ingest(batch(3, 0), t0 + DEADLINE);
        long elapsed = System.nanoTime() - t0;
        assertThat(outcome).isEqualTo(IngestPath.Outcome.PRODUCE_TIMEOUT);
        assertThat(Duration.ofNanos(elapsed)).isBetween(Duration.ofMillis(380), Duration.ofMillis(600));
    }

    @Test
    void synchronousSendExceptionIsFailureNotCrash() throws Exception {
        producer.sendException = new KafkaException("Topic not present in metadata after 500 ms");
        assertThat(path.ingest(batch(1, 0), System.nanoTime() + DEADLINE)).isEqualTo(IngestPath.Outcome.PRODUCE_FAILED);
    }

    @Test
    void noBudgetLeftMeansNoProduce() throws Exception {
        var outcome = path.ingest(batch(2, 0), System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(5));
        assertThat(outcome).isEqualTo(IngestPath.Outcome.DEADLINE_EXHAUSTED);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void breakerOpensAfterFailuresAndThenRejectsInstantly() throws Exception {
        producer.sendException = new KafkaException("down");
        for (int i = 0; i < 4; i++) path.ingest(batch(1, 0), System.nanoTime() + DEADLINE);
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        long t0 = System.nanoTime();
        assertThat(path.ingest(batch(1, 0), System.nanoTime() + DEADLINE)).isEqualTo(IngestPath.Outcome.BREAKER_OPEN);
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(20));
    }
}
