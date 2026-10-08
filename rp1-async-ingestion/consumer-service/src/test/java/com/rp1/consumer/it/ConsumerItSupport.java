package com.rp1.consumer.it;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;
import com.rp1.common.Json;
import com.rp1.common.Topics;
import com.rp1.testkit.KafkaBehindToxiproxy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared containers + helpers for consumer ITs. */
final class ConsumerItSupport {
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");
    static KafkaProducer<String, byte[]> producer;
    static JdbcTemplate jdbc;
    private static final EnvelopeFactory FACTORY = new EnvelopeFactory(Duration.ofMinutes(5));

    private ConsumerItSupport() {}

    static synchronized void start() throws Exception {
        if (producer != null) return;
        Map<String, Integer> topics = new LinkedHashMap<>();
        topics.put(Topics.CALLBACK_EVENTS, 12);
        for (int i = 0; i < 3; i++) topics.put(Topics.CALLBACK_EVENTS + "-retry-" + i, 12);
        topics.put(Topics.CALLBACK_EVENTS_DLT, 3);
        KafkaBehindToxiproxy.start(topics);
        PG.start();
        producer = new KafkaProducer<>(Map.of("bootstrap.servers", KafkaBehindToxiproxy.directBootstrap(), "acks", "all",
                "linger.ms", 5), new StringSerializer(), new ByteArraySerializer());
        jdbc = new JdbcTemplate(new DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
    }

    /** Spring args for a consumer instance pointed at the test containers. */
    static List<String> args(String group, String... extra) {
        List<String> a = new ArrayList<>(List.of(
                "--server.port=0",
                "--spring.kafka.bootstrap-servers=" + KafkaBehindToxiproxy.proxiedBootstrap(),
                "--spring.kafka.consumer.group-id=" + group,
                "--spring.datasource.url=" + PG.getJdbcUrl(),
                "--spring.datasource.username=" + PG.getUsername(),
                "--spring.datasource.password=" + PG.getPassword(),
                "--rp1.downstream.fault-markers=true",
                "--rp1.consumer.retry.initial-backoff=300ms",
                "--rp1.consumer.retry.max-backoff=1s",
                "--rp1.consumer.lag-monitor.enabled=false"));
        a.addAll(List.of(extra));
        return a;
    }

    static CallbackEnvelope envelope(String type, String sub, String msg, long ts) {
        try {
            var node = Json.MAPPER.readTree("""
                    {"eventCategoryType":"TransactionalSendEvents.%s","timestampUTC":%d,
                     "info":{"subscriberKey":"%s","messageKey":"%s"}}""".formatted(type, ts, sub, msg));
            return ((EnvelopeFactory.Valid) FACTORY.build(node, "it-batch", Instant.now(), "it-trace")).envelope();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static void send(CallbackEnvelope e) {
        try {
            producer.send(new ProducerRecord<>(Topics.CALLBACK_EVENTS, e.subscriberKey(), Json.MAPPER.writeValueAsBytes(e))).get();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    static void sendRaw(String key, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(Topics.CALLBACK_EVENTS, key, value)).get();
    }

    static int count(String sql, Object... args) {
        Integer n = jdbc.queryForObject(sql, Integer.class, args);
        return n == null ? 0 : n;
    }
}
