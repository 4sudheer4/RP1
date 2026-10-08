package com.rp1.common;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * Validates one SFMC event and wraps it in a {@link CallbackEnvelope}. Pure function, shared by ingest (to build
 * envelopes) and loadgen (to compute the same event ids for the ledger).
 *
 * <p>Validation is per event, never per batch: one permanently bad event must not make the whole batch fail,
 * or SFMC would retry that batch until it drops it, losing the good events too.
 */
public final class EnvelopeFactory {

    /** Why an event was routed to {@code invalid-events}. Values are metric tags, keep them stable. */
    public enum InvalidReason { NOT_OBJECT, MISSING_EVENT_TYPE, UNSUPPORTED_EVENT_TYPE, MISSING_SUBSCRIBER_KEY, MISSING_MESSAGE_KEY }

    public sealed interface Result permits Valid, Invalid {}
    public record Valid(CallbackEnvelope envelope) implements Result {}
    public record Invalid(InvalidReason reason, JsonNode event) implements Result {}

    private static final int MAX_UPSTREAM_ID_LEN = 128;
    private static final int MAX_KEY_LEN = 512;

    private final Duration maxClockSkew;

    public EnvelopeFactory(Duration maxClockSkew) {
        this.maxClockSkew = maxClockSkew;
    }

    public Result build(JsonNode event, String batchId, Instant receivedAt, String traceId) {
        if (event == null || !event.isObject()) return new Invalid(InvalidReason.NOT_OBJECT, event);

        String wireType = text(event, "eventCategoryType");
        if (wireType == null) return new Invalid(InvalidReason.MISSING_EVENT_TYPE, event);
        Optional<EventType> type = EventType.fromWire(wireType);
        if (type.isEmpty()) return new Invalid(InvalidReason.UNSUPPORTED_EVENT_TYPE, event);

        JsonNode info = event.path("info");
        String subscriberKey = bounded(text(info, "subscriberKey"));
        if (subscriberKey == null) return new Invalid(InvalidReason.MISSING_SUBSCRIBER_KEY, event);
        String messageKey = bounded(text(info, "messageKey"));
        if (messageKey == null && type.get().affectsStatus()) return new Invalid(InvalidReason.MISSING_MESSAGE_KEY, event);

        Instant upstreamTs = parseTimestamp(event.get("timestampUTC"));
        Instant latestTrusted = receivedAt.plus(maxClockSkew);
        boolean skewed = upstreamTs != null && upstreamTs.isAfter(latestTrusted);
        Instant orderingTs = upstreamTs == null ? receivedAt : (skewed ? latestTrusted : upstreamTs);

        return new Valid(new CallbackEnvelope(eventId(event), batchId, receivedAt, CallbackEnvelope.CURRENT_SCHEMA,
                traceId, upstreamTs, orderingTs, skewed, wireType, subscriberKey, messageKey, event));
    }

    /** Upstream id if the source provides a usable one, else content hash. SFMC ENS: always the hash. */
    public static String eventId(JsonNode event) {
        String upstream = text(event, "eventId");
        if (upstream != null && upstream.length() <= MAX_UPSTREAM_ID_LEN) return "u:" + upstream;
        return Canonical.eventId(event);
    }

    /** ENS docs type timestampUTC as epoch millis; some integrations send ISO-8601. Accept both, else null. */
    static Instant parseTimestamp(JsonNode ts) {
        if (ts == null || ts.isNull()) return null;
        if (ts.isIntegralNumber()) return Instant.ofEpochMilli(ts.asLong());
        if (ts.isTextual()) {
            try {
                return Instant.parse(ts.asText());
            } catch (DateTimeParseException e) {
                return null;
            }
        }
        return null;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v != null && v.isTextual() && !v.asText().isBlank() ? v.asText() : null;
    }

    private static String bounded(String s) {
        return s == null || s.length() > MAX_KEY_LEN ? null : s;
    }
}
