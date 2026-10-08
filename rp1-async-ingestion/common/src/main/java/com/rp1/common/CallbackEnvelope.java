package com.rp1.common;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * One SFMC event plus ingest metadata: the value written to {@code callback-events}.
 *
 * @param eventId        idempotency key: upstream id if present, else SHA-256 of canonical event JSON
 * @param batchId        id of the HTTP batch this event arrived in (diagnostics only, never a partition key)
 * @param receivedAt     ingest wall clock when the batch was accepted
 * @param schemaVersion  envelope schema version (see {@link #CURRENT_SCHEMA})
 * @param traceId        W3C trace id from {@code traceparent}, or generated at ingest
 * @param upstreamTs     SFMC {@code timestampUTC} as sent (null if absent/unparseable); informational
 * @param orderingTs     timestamp used for same-rank tie-breaks: upstreamTs clamped to receivedAt + max skew
 * @param clockSkewed    true if upstreamTs was beyond the skew tolerance and got clamped
 * @param eventType      SFMC {@code eventCategoryType}
 * @param subscriberKey  partition key (per-subscriber ordering)
 * @param messageKey     state-machine key (one transactional message)
 * @param event          the original SFMC event JSON, unknown fields included
 */
public record CallbackEnvelope(
        String eventId,
        String batchId,
        Instant receivedAt,
        int schemaVersion,
        String traceId,
        Instant upstreamTs,
        Instant orderingTs,
        boolean clockSkewed,
        String eventType,
        String subscriberKey,
        String messageKey,
        JsonNode event) {

    public static final int CURRENT_SCHEMA = 1;
}
