package com.rp1.consumer.infra;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Lag in records is not enough to alert on: 10k records behind can be 1s or 1h. This samples, per partition,
 * the timestamp of the first uncommitted record (the record at the group's committed offset) and exposes
 * {@code rp1.consumer.oldest.unprocessed.age} (max across partitions, seconds) plus total lag. Read-only: a
 * group-less consumer that assigns and seeks, never commits.
 */
public class LagAgeMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LagAgeMonitor.class);

    private final String groupId;
    private final String topic;
    private final AdminClient admin;
    private final KafkaConsumer<byte[], byte[]> peeker;
    private volatile double oldestAgeSeconds;
    private volatile double totalLag;

    public LagAgeMonitor(String bootstrap, String groupId, String topic, MeterRegistry registry) {
        this.groupId = groupId;
        this.topic = topic;
        this.admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap, "client.id", "rp1-lag-monitor"));
        this.peeker = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", bootstrap,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
                ConsumerConfig.CLIENT_ID_CONFIG, "rp1-lag-peeker"), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        Gauge.builder("rp1.consumer.oldest.unprocessed.age", () -> oldestAgeSeconds).baseUnit("seconds").tag("topic", topic).register(registry);
        Gauge.builder("rp1.consumer.lag", () -> totalLag).tag("topic", topic).register(registry);
    }

    /** Called on a schedule. Errors are logged and the previous values kept (stale beats wrong). */
    public synchronized void sample() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (committed.isEmpty()) return;
            Map<TopicPartition, OffsetSpec> latestReq = new HashMap<>();
            committed.keySet().forEach(tp -> latestReq.put(tp, OffsetSpec.latest()));
            var ends = admin.listOffsets(latestReq).all().get(5, TimeUnit.SECONDS);

            long now = System.currentTimeMillis();
            double maxAge = 0;
            long lag = 0;
            for (var e : committed.entrySet()) {
                TopicPartition tp = e.getKey();
                long pos = e.getValue().offset();
                long end = ends.get(tp).offset();
                if (end <= pos) continue;
                lag += end - pos;
                peeker.assign(List.of(tp));
                peeker.seek(tp, pos);
                for (ConsumerRecord<byte[], byte[]> r : peeker.poll(Duration.ofSeconds(2)).records(tp)) {
                    maxAge = Math.max(maxAge, (now - r.timestamp()) / 1000.0);
                    break;
                }
            }
            oldestAgeSeconds = maxAge;
            totalLag = lag;
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            log.warn("lag/age sample failed: {}", e.toString());
        }
    }

    @Override
    public void close() {
        peeker.close(Duration.ofSeconds(2));
        admin.close(Duration.ofSeconds(2));
    }
}
