package com.rp1.consumer.engine;

import com.rp1.consumer.config.ConsumerProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Backpressure toward the downstream: when it answers 429, pause the main listener container (all its
 * partitions) for a jittered, exponentially growing window, then resume. The consumer keeps polling while paused,
 * so heartbeats and max.poll.interval.ms are unaffected: no rebalance. Successes reset the backoff.
 */
public class ThrottleGuard {
    private static final Logger log = LoggerFactory.getLogger(ThrottleGuard.class);

    private final KafkaListenerEndpointRegistry registry;
    private final TaskScheduler scheduler;
    private final ConsumerProperties.Throttle cfg;
    private final String containerId;
    private final AtomicInteger consecutive = new AtomicInteger();
    private final AtomicBoolean paused = new AtomicBoolean();
    private final Counter pauses;

    public ThrottleGuard(KafkaListenerEndpointRegistry registry, TaskScheduler scheduler, ConsumerProperties.Throttle cfg,
                         String containerId, MeterRegistry meters) {
        this.registry = registry;
        this.scheduler = scheduler;
        this.cfg = cfg;
        this.containerId = containerId;
        this.pauses = Counter.builder("rp1.consumer.throttle.pauses").register(meters);
    }

    public void onThrottled() {
        int n = consecutive.incrementAndGet();
        if (!paused.compareAndSet(false, true)) return;
        MessageListenerContainer c = registry.getListenerContainer(containerId);
        if (c == null) {
            paused.set(false);
            return;
        }
        long base = Math.min(cfg.maxPause().toMillis(), cfg.initialPause().toMillis() << Math.min(n - 1, 10));
        long pauseMs = base / 2 + ThreadLocalRandom.current().nextLong(base / 2 + 1); // "equal jitter"
        c.pause();
        pauses.increment();
        log.warn("downstream throttling: pausing {} for {}ms (consecutive={})", containerId, pauseMs, n);
        scheduler.schedule(() -> {
            c.resume();
            paused.set(false);
        }, Instant.now().plus(Duration.ofMillis(pauseMs)));
    }

    public void onSuccess() {
        if (consecutive.get() != 0) consecutive.set(0);
    }
}
