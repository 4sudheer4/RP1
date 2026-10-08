package com.rp1.consumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("rp1.consumer")
public record ConsumerProperties(
        /* main listener threads; 12 partitions / concurrency should be an integer */
        int concurrency,
        /* client-side cap on downstream calls per second across this instance (0 = off) */
        double maxRps,
        Retry retry,
        Throttle throttle) {

    /**
     * Non-blocking retries: attempts includes the first try, so attempts=4 -> 3 retry topics
     * (callback-events-retry-0..2) with exponential backoff + jitter, then callback-events-dlt.
     */
    public record Retry(int attempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {}

    /** On a downstream 429 the main container is paused for an exponentially growing, jittered window. */
    public record Throttle(Duration initialPause, Duration maxPause) {}
}
