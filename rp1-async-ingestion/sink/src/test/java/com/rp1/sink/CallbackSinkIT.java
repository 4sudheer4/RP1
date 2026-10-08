package com.rp1.sink;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;
import com.rp1.common.Json;
import com.rp1.common.errors.NonRetryableException;
import com.rp1.common.errors.RetryableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(classes = CallbackSinkIT.App.class, properties = "rp1.downstream.fault-markers=true")
class CallbackSinkIT {

    @SpringBootApplication
    @Import(SinkConfiguration.class)
    static class App {
        @org.springframework.context.annotation.Bean
        io.micrometer.core.instrument.MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
    }

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16.4-alpine");

    @Autowired CallbackSink sink;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;

    private final EnvelopeFactory factory = new EnvelopeFactory(Duration.ofMinutes(5));
    private final Instant now = Instant.now();

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE callback_event, message_status, invalid_event, downstream_call");
    }

    private CallbackEnvelope event(String type, String sub, String msg, long ts) {
        try {
            var node = Json.MAPPER.readTree("""
                    {"eventCategoryType":"TransactionalSendEvents.%s","timestampUTC":%d,
                     "info":{"subscriberKey":"%s","messageKey":"%s"}}""".formatted(type, ts, sub, msg));
            return ((EnvelopeFactory.Valid) factory.build(node, "b1", now, "t1")).envelope();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String status(String msg) {
        return jdbc.queryForObject("SELECT status FROM message_status WHERE message_key = ?", String.class, msg);
    }

    @Test
    void duplicateDeliveryIsAbsorbed() {
        var e = event("EmailSent", "s1", "m1", 1000);
        assertThat(sink.process(e, CallbackSink.Source.kafka(0, 1))).isEqualTo(CallbackSink.Outcome.APPLIED);
        assertThat(sink.process(e, CallbackSink.Source.kafka(0, 1))).isEqualTo(CallbackSink.Outcome.DUPLICATE);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM callback_event", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT transitions FROM message_status", Integer.class)).isEqualTo(1);
    }

    @Test
    void lateSentNeverOverwritesBounced() {
        sink.process(event("EmailBounced", "s1", "m1", 2000), CallbackSink.Source.SYNC);
        var outcome = sink.process(event("EmailSent", "s1", "m1", 1000), CallbackSink.Source.SYNC);
        assertThat(outcome).isEqualTo(CallbackSink.Outcome.STALE_STATUS_IGNORED);
        assertThat(status("m1")).isEqualTo("BOUNCED");
        // the late event is still recorded in the log: legitimate events are never dropped
        assertThat(jdbc.queryForObject("SELECT count(*) FROM callback_event", Integer.class)).isEqualTo(2);
    }

    @Test
    void finalStateIsIndependentOfArrivalOrder() {
        List<CallbackEnvelope> events = List.of(
                event("SmsSent", "s1", "m1", 1000),
                event("SmsTransient", "s1", "m1", 2000),
                event("SmsTransient", "s1", "m1", 3000),
                event("SmsDelivered", "s1", "m1", 4000));
        for (int round = 0; round < 10; round++) {
            clean();
            List<CallbackEnvelope> shuffled = new ArrayList<>(events);
            Collections.shuffle(shuffled);
            shuffled.forEach(e -> sink.process(e, CallbackSink.Source.SYNC));
            assertThat(status("m1")).isEqualTo("DELIVERED");
            assertThat(jdbc.queryForObject("SELECT last_event_id FROM message_status", String.class))
                    .isEqualTo(events.get(3).eventId());
        }
    }

    @Test
    void downstreamFailureRollsBackSoRetryCanSucceed() {
        var flaky = event("EmailSent", "flaky-1", "m1", 1000);
        assertThatThrownBy(() -> sink.process(flaky, CallbackSink.Source.SYNC)).isInstanceOf(RetryableException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM callback_event", Integer.class)).isZero();

        var poison = event("EmailSent", "poison-1", "m2", 1000);
        assertThatThrownBy(() -> sink.process(poison, CallbackSink.Source.SYNC)).isInstanceOf(NonRetryableException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM callback_event", Integer.class)).isZero();
    }

    @Test
    void concurrentConsumersSameEventExactlyOneApplies() throws Exception {
        // Downstream that blocks the first caller so the second INSERT must wait on the uncommitted row.
        CountDownLatch firstInside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Boolean> first = new AtomicReference<>(true);
        Downstream slow = env -> {
            if (first.getAndSet(false)) {
                firstInside.countDown();
                try { release.await(); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            }
        };
        CallbackSink racing = new CallbackSink(jdbc, tx, slow, new SimpleMeterRegistry());
        var e = event("EmailSent", "s1", "m1", 1000);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<CallbackSink.Outcome> a = pool.submit(() -> racing.process(e, CallbackSink.Source.kafka(0, 1)));
        firstInside.await();
        Future<CallbackSink.Outcome> b = pool.submit(() -> racing.process(e, CallbackSink.Source.kafka(3, 9)));
        Thread.sleep(300); // b is now blocked on the PK lock held by a's open txn
        release.countDown();
        assertThat(List.of(a.get(), b.get()))
                .containsExactlyInAnyOrder(CallbackSink.Outcome.APPLIED, CallbackSink.Outcome.DUPLICATE);
        pool.shutdown();
    }
}
