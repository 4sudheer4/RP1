package com.rp1.sink;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Simulated downstream (stands in for the SFMC Data Extension API and other per-event work the old sync handler
 * did). A fixed concurrency limit + service time is what gives the system a finite capacity; that capacity is
 * the same in sync and async mode so the comparison is apples to apples.
 */
@ConfigurationProperties("rp1.downstream")
public record SinkProperties(
        /* concurrent calls the downstream accepts (like a DE API concurrency limit) */
        int capacity,
        /* time each call holds a permit */
        Duration serviceTime,
        /* +/- uniform jitter on serviceTime */
        Duration jitter,
        /* how long a caller waits for a permit before treating it as a 429 */
        Duration acquireTimeout,
        /* probability [0,1] of an injected transient failure per call */
        double transientFailureRate,
        /* enables subscriberKey-prefix fault markers: "poison-" (permanent 4xx), "flaky-" (always 5xx) */
        boolean faultMarkers,
        /* record per-event call counts in downstream_call (experiment instrumentation) */
        boolean recordCalls) {

    public SinkProperties {
        if (capacity <= 0) capacity = 8;
        if (serviceTime == null) serviceTime = Duration.ofMillis(2);
        if (jitter == null) jitter = Duration.ZERO;
        if (acquireTimeout == null) acquireTimeout = Duration.ofSeconds(1);
    }
}
