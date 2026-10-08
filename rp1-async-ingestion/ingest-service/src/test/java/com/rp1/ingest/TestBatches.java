package com.rp1.ingest;

import com.rp1.ingest.config.IngestProperties;
import com.rp1.ingest.config.IngestProperties.Signature.KeyEncoding;

import java.time.Duration;
import java.util.List;
import java.util.StringJoiner;

/** SFMC-shaped test payloads (field names from the official "Supported Notification Events" examples). */
public final class TestBatches {
    public static final String KEY = "V27FXfqI3DnhfQW1bhFDeJixpt8eDAY5R24UJI3cK6M=";
    public static final String OLD_KEY = "KktBXb9V556KtSDG7Wb5a0xHAu/z9n3k+YekDzLy7i0=";

    private TestBatches() {}

    public static String event(String type, String subscriber, String message, long ts) {
        return """
                {"eventCategoryType":"TransactionalSendEvents.%s","timestampUTC":%d,"compositeId":"c.%s",
                 "definitionKey":"appt","mid":1447640,"eid":1447640,
                 "info":{"to":"%s@example.test","subscriberKey":"%s","messageKey":"%s","status":"x"}}"""
                .formatted(type, ts, message, subscriber, subscriber, message);
    }

    public static String batch(int n, String prefix) {
        StringJoiner j = new StringJoiner(",", "[", "]");
        for (int i = 0; i < n; i++) j.add(event("EmailSent", prefix + "-sub-" + (i % 7), prefix + "-msg-" + i, 1_700_000_000_000L + i));
        return j.toString();
    }

    public static IngestProperties props(int maxInFlight, Duration deadline) {
        return new IngestProperties(IngestProperties.Mode.ASYNC, deadline, Duration.ofMillis(50), 64 * 1024, 100,
                maxInFlight, Duration.ofMinutes(5), 1,
                new IngestProperties.Signature(true, List.of(KEY, OLD_KEY), KeyEncoding.UTF8),
                new IngestProperties.Verification(true, "cb-1"),
                new IngestProperties.Breaker(50, 10, 5, Duration.ofSeconds(5), Duration.ofSeconds(1), 80),
                new IngestProperties.Sync(Duration.ofSeconds(2)));
    }
}
