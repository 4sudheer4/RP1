package com.rp1.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties("rp1.ingest")
public record IngestProperties(
        Mode mode,
        /* Handler budget. SFMC ENS allows 2s per batch (official "Retries and Callback Suspensions"); we answer
           by 1.5s so network + TLS + SFMC-side measurement still land inside it. */
        Duration hardDeadline,
        /* Below this remaining budget we do not start producing: we could not finish anyway. */
        Duration minProduceBudget,
        int maxBodyBytes,
        /* SFMC maxBatchSize is 100..1000 */
        int maxBatchEvents,
        /* Bulkhead: concurrent requests past admission. Excess gets 503 immediately instead of queueing. */
        int maxInFlight,
        Duration maxClockSkew,
        int retryAfterSeconds,
        Signature signature,
        Verification verification,
        Breaker breaker,
        Sync sync) {

    public enum Mode { ASYNC, SYNC }

    /**
     * @param keys        HMAC keys, current first then previous (rotation window: SFMC's ens-regenerate
     *                    deactivates the old key immediately, so accept both until config rolls out everywhere)
     * @param keyEncoding how the SFMC signatureKey string becomes HMAC key bytes. The official docs do not say;
     *                    UTF8 (use the string's bytes) is the default, BASE64 decodes it. Must be confirmed against
     *                    a real SFMC callback before production.
     */
    public record Signature(boolean enabled, List<String> keys, KeyEncoding keyEncoding) {
        public enum KeyEncoding { UTF8, BASE64 }
    }

    /**
     * ENS callback verification handshake: an unsigned POST {"callbackId","verificationKey"}. Only accepted while
     * enabled (turn on during callback registration, off afterwards). expectedCallbackId pins the callback.
     */
    public record Verification(boolean enabled, String expectedCallbackId) {}

    public record Breaker(float failureRateThreshold, int slidingWindowSize, int minimumNumberOfCalls,
                          Duration waitInOpenState, Duration slowCallThreshold, float slowCallRateThreshold) {}

    /** Sync baseline: the SFMC-side timeout we compare against to count work done for abandoned requests. */
    public record Sync(Duration clientTimeout) {}
}
