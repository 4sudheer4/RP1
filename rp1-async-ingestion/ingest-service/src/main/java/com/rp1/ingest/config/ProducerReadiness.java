package com.rp1.ingest.config;

import com.rp1.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Readiness gate: the instance reports ready only after the producer has fetched metadata for callback-events,
 * so a cold instance does not take traffic it can only answer with 503. Part of the readiness group only
 * (liveness must not depend on Kafka, or a Kafka outage would restart every pod).
 *
 * <p>Limit: this proves metadata, not that the idempotent producer has its producer id; that wait is still
 * bounded by the per-request deadline.
 */
public class ProducerReadiness implements HealthIndicator {
    private static final Logger log = LoggerFactory.getLogger(ProducerReadiness.class);
    private final KafkaTemplate<String, byte[]> kafka;
    private final AtomicBoolean warm = new AtomicBoolean(false);

    ProducerReadiness(KafkaTemplate<String, byte[]> kafka) {
        this.kafka = kafka;
    }

    @EventListener(ApplicationReadyEvent.class)
    void warmUp() {
        Thread.ofVirtual().name("producer-warmup").start(() -> {
            while (!warm.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    int partitions = kafka.partitionsFor(Topics.CALLBACK_EVENTS).size();
                    warm.set(true);
                    log.info("producer warm: {} partitions for {}", partitions, Topics.CALLBACK_EVENTS);
                } catch (RuntimeException e) {
                    log.warn("producer warm-up failed, retrying: {}", e.toString());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        });
    }

    @Override
    public Health health() {
        return warm.get() ? Health.up().build() : Health.down().withDetail("reason", "producer metadata not fetched yet").build();
    }
}
