package com.rp1.loadgen;

import com.rp1.common.Headers;
import com.rp1.common.Json;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-publishes DLT records to the main topic, with the original key so they land on the subscriber's partition.
 * Safe to run any number of times: replayed events go through the same idempotent sink (already-applied ids are
 * no-ops) and the status guard (an old event cannot regress a newer status). Progress is tracked with a
 * consumer group and committed only after the re-publish is acked (acks=all), so a crash re-replays, never skips.
 * Records whose bytes are not a valid envelope (deserialization poison) are skipped and counted: they need a
 * human, not a retry.
 */
final class DlqReplay {
    record Config(String bootstrap, String from, String to, String group, long max, boolean dryRun) {}

    private final Config cfg;

    DlqReplay(Config cfg) {
        this.cfg = cfg;
    }

    Map<String, Object> run() throws Exception {
        long replayed = 0, skipped = 0;
        try (var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
                "bootstrap.servers", cfg.bootstrap(),
                ConsumerConfig.GROUP_ID_CONFIG, cfg.group(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200), new ByteArrayDeserializer(), new ByteArrayDeserializer());
             var producer = new KafkaProducer<byte[], byte[]>(Map.of(
                     "bootstrap.servers", cfg.bootstrap(), "acks", "all", "enable.idempotence", true),
                     new ByteArraySerializer(), new ByteArraySerializer())) {
            consumer.subscribe(List.of(cfg.from()));
            int idlePolls = 0;
            while (idlePolls < 5 && replayed < cfg.max()) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    idlePolls++;
                    continue;
                }
                idlePolls = 0;
                Map<TopicPartition, OffsetAndMetadata> commit = new LinkedHashMap<>();
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    if (replayed >= cfg.max()) break;
                    commit.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                    if (!isEnvelope(r.value())) {
                        skipped++;
                        continue;
                    }
                    if (!cfg.dryRun()) {
                        ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(cfg.to(), r.key(), r.value());
                        String origin = r.topic() + "-" + r.partition() + "@" + r.offset();
                        out.headers().add(new RecordHeader(Headers.REPLAYED_FROM, origin.getBytes(StandardCharsets.UTF_8)));
                        for (String h : List.of(Headers.EVENT_ID, Headers.TRACE_ID, Headers.BATCH_ID)) {
                            var v = r.headers().lastHeader(h);
                            if (v != null) out.headers().add(v);
                        }
                        producer.send(out).get(); // ack before commit: crash => replay again, never skip
                    }
                    replayed++;
                }
                if (!cfg.dryRun()) consumer.commitSync(commit);
            }
        }
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("from", cfg.from());
        s.put("to", cfg.to());
        s.put("dryRun", cfg.dryRun());
        s.put("replayed", replayed);
        s.put("skippedNotAnEnvelope", skipped);
        return s;
    }

    private static boolean isEnvelope(byte[] v) {
        try {
            var n = v == null ? null : Json.MAPPER.readTree(v);
            return n != null && n.hasNonNull("eventId") && n.hasNonNull("subscriberKey") && n.hasNonNull("event");
        } catch (Exception e) {
            return false;
        }
    }
}
