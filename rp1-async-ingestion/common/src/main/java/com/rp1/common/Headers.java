package com.rp1.common;

/** Kafka record headers set by ingest. Envelope fields are duplicated here so tooling can route without parsing. */
public final class Headers {
    public static final String EVENT_ID = "rp1-event-id";
    public static final String BATCH_ID = "rp1-batch-id";
    public static final String TRACE_ID = "rp1-trace-id";
    public static final String SCHEMA_VERSION = "rp1-schema-version";
    /** Set by the DLQ replay tool so replayed records are distinguishable in metrics and logs. */
    public static final String REPLAYED_FROM = "rp1-replayed-from";
    /** Added to DLT records (alongside Spring's kafka_dlt-* headers). */
    public static final String FIRST_FAILURE_TS = "rp1-first-failure-ts";
    public static final String ATTEMPTS = "rp1-attempts";
    public static final String ERROR_CLASS = "rp1-error-class";

    private Headers() {}
}
