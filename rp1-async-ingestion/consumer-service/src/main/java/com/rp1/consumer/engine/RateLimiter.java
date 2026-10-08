package com.rp1.consumer.engine;

import com.rp1.common.errors.ThrottledException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Token bucket shared by all listener threads: keeps this instance under the downstream's published rate limit
 * instead of discovering it through 429s. Waits at most {@code maxWait} for a token, then reports throttling.
 */
public final class RateLimiter {
    private final double permitsPerNano;
    private final double burst;
    private final long maxWaitNanos;
    private double tokens;
    private long last = System.nanoTime();

    public RateLimiter(double perSecond, Duration maxWait) {
        this.permitsPerNano = perSecond / 1e9;
        this.burst = Math.max(1, perSecond / 10);
        this.maxWaitNanos = maxWait.toNanos();
        this.tokens = burst;
    }

    public void acquire() {
        long waitNanos;
        synchronized (this) {
            long now = System.nanoTime();
            tokens = Math.min(burst, tokens + (now - last) * permitsPerNano);
            last = now;
            tokens -= 1;
            waitNanos = tokens >= 0 ? 0 : (long) (-tokens / permitsPerNano);
            if (waitNanos > maxWaitNanos) {
                tokens += 1; // give the token back: this caller is not proceeding
                throw new ThrottledException("client-side rate limit");
            }
        }
        if (waitNanos > 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(waitNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ThrottledException("interrupted waiting for rate limit");
            }
        }
    }
}
