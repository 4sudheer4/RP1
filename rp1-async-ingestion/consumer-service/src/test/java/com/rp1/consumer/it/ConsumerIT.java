package com.rp1.consumer.it;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.Headers;
import com.rp1.common.Topics;
import com.rp1.consumer.ConsumerApplication;
import com.rp1.testkit.KafkaBehindToxiproxy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.rp1.consumer.it.ConsumerItSupport.count;
import static com.rp1.consumer.it.ConsumerItSupport.envelope;
import static com.rp1.consumer.it.ConsumerItSupport.send;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConsumerIT {

    @BeforeAll
    static void infra() throws Exception {
        ConsumerItSupport.start();
    }

    private static ConfigurableApplicationContext startConsumer(String group, String... extra) {
        return new SpringApplicationBuilder(ConsumerApplication.class).profiles("textlogs")
                .run(ConsumerItSupport.args(group, extra).toArray(String[]::new));
    }

    private static String header(ConsumerRecord<?, ?> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void duplicatesAndOutOfOrderDeliveryGiveExactlyOneRowPerEventAndCorrectFinalState() {
        String p = "ooo-" + UUID.randomUUID().toString().substring(0, 8);
        List<CallbackEnvelope> events = new ArrayList<>();
        for (int m = 0; m < 50; m++) {
            events.add(envelope("EmailSent", p + "-s" + (m % 5), p + "-m" + m, 1000));
            events.add(envelope("EmailBounced", p + "-s" + (m % 5), p + "-m" + m, 2000));
        }
        List<CallbackEnvelope> wire = new ArrayList<>(events);
        wire.addAll(events);               // every event delivered twice (SFMC retry / redelivery)
        Collections.shuffle(wire);         // and out of order (bounced before sent)
        wire.forEach(ConsumerItSupport::send);

        try (var app = startConsumer("g-" + p)) {
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%") == 100);
            // let the duplicates drain too, then check nothing extra appeared
            await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> true);
            assertThat(count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%")).isEqualTo(100);
            assertThat(count("SELECT count(*) FROM message_status WHERE message_key LIKE ? AND status = 'BOUNCED'", p + "%")).isEqualTo(50);
        }
    }

    @Test
    void poisonAndUndeserializableRecordsGoToDltWithoutBlockingTheirPartition() throws Exception {
        String p = "poison-" + UUID.randomUUID().toString().substring(0, 8);
        String key = p + "-s1"; // "poison-" prefix => downstream permanent 400 (fault marker)
        send(envelope("EmailSent", key, p + "-bad", 1));
        ConsumerItSupport.sendRaw(key, "{not json".getBytes(StandardCharsets.UTF_8));
        // same key -> same partition, queued behind the poison records
        String goodKey = "good-" + p;
        for (int i = 0; i < 20; i++) send(envelope("EmailSent", goodKey, p + "-ok" + i, i));

        try (var app = startConsumer("g-" + p)) {
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key = ?", goodKey) == 20);
        }
        // Other tests' consumer groups also read the shared topic from the earliest offset and may DLT these
        // records again, so assert on distinct original offsets rather than raw DLT record count.
        List<ConsumerRecord<String, byte[]>> dlt = KafkaBehindToxiproxy.readAll(Topics.CALLBACK_EVENTS_DLT).stream()
                .filter(r -> key.equals(r.key())).toList();
        assertThat(dlt.stream().map(r -> header(r, "kafka_original-offset") == null ? "?"
                : Long.toString(ByteBuffer.wrap(r.headers().lastHeader("kafka_original-offset").value()).getLong())).distinct())
                .hasSize(2);
        for (var r : dlt) {
            assertThat(header(r, "kafka_original-topic")).isEqualTo(Topics.CALLBACK_EVENTS);
            assertThat(r.headers().lastHeader("kafka_original-offset")).isNotNull();
            assertThat(header(r, Headers.ERROR_CLASS)).isEqualTo("non_retryable");
            assertThat(ByteBuffer.wrap(r.headers().lastHeader(Headers.ATTEMPTS).value()).getInt()).isEqualTo(1); // no retries
            assertThat(header(r, Headers.FIRST_FAILURE_TS)).isNotNull();
        }
        assertThat(dlt).anySatisfy(r -> assertThat(new String(r.value(), StandardCharsets.UTF_8)).isEqualTo("{not json"));
        assertThat(count("SELECT count(*) FROM callback_event WHERE subscriber_key = ?", key)).isZero();
    }

    @Test
    void transientFailureRetriesThroughRetryTopicsThenDlt() {
        String p = "flaky-" + UUID.randomUUID().toString().substring(0, 8); // "flaky-" => always 503
        send(envelope("EmailSent", p, p + "-m", 1));
        try (var app = startConsumer("g-" + p)) {
            await().atMost(Duration.ofSeconds(45)).until(() -> KafkaBehindToxiproxy.readAll(Topics.CALLBACK_EVENTS_DLT).stream()
                    .anyMatch(r -> p.equals(r.key())));
        }
        var r = KafkaBehindToxiproxy.readAll(Topics.CALLBACK_EVENTS_DLT).stream().filter(x -> p.equals(x.key())).findFirst().orElseThrow();
        assertThat(header(r, Headers.ERROR_CLASS)).isEqualTo("retryable");
        assertThat(ByteBuffer.wrap(r.headers().lastHeader(Headers.ATTEMPTS).value()).getInt()).isEqualTo(4); // 1 + 3 retries
        for (int i = 0; i < 3; i++) {
            int idx = i;
            assertThat(KafkaBehindToxiproxy.readAll(Topics.CALLBACK_EVENTS + "-retry-" + i))
                    .as("passed through retry-" + idx).anySatisfy(x -> assertThat(x.key()).isEqualTo(p));
        }
    }

    @Test
    void rebalanceMidStreamLosesNothingAndDuplicatesAreAbsorbed() throws Exception {
        String p = "reb-" + UUID.randomUUID().toString().substring(0, 8);
        int messages = 600; // x2 events (sent + bounced) = 1200 events
        String group = "g-" + p;
        var a = startConsumer(group, "--rp1.downstream.service-time=5ms");
        Thread producerThread = Thread.ofVirtual().start(() -> {
            for (int m = 0; m < messages; m++) {
                send(envelope("EmailSent", p + "-s" + (m % 40), p + "-m" + m, 1000));
                send(envelope("EmailBounced", p + "-s" + (m % 40), p + "-m" + m, 2000));
            }
        });
        await().atMost(Duration.ofSeconds(30)).until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%") > 100);
        var b = startConsumer(group, "--rp1.downstream.service-time=5ms");   // join: partitions move to b
        await().atMost(Duration.ofSeconds(30)).until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%") > 600);
        a.close();                                                            // leave: partitions move again
        producerThread.join();
        try (b) {
            await().atMost(Duration.ofSeconds(90))
                    .until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%") == 2 * messages);
        }
        assertThat(count("SELECT count(*) FROM message_status WHERE message_key LIKE ? AND status = 'BOUNCED'", p + "%")).isEqualTo(messages);
    }
}
