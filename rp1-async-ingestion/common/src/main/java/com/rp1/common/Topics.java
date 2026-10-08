package com.rp1.common;

/** Topic names. Must match infra/kafka/create-topics.sh (broker auto-create is off, so a typo fails loudly). */
public final class Topics {
    public static final String CALLBACK_EVENTS = "callback-events";
    public static final String INVALID_EVENTS = "invalid-events";
    /** Non-blocking retry topics: callback-events-retry-0..N-1 (Spring Kafka index-suffix strategy). */
    public static final String RETRY_SUFFIX = "-retry";
    public static final String DLT_SUFFIX = "-dlt";
    public static final String CALLBACK_EVENTS_DLT = CALLBACK_EVENTS + DLT_SUFFIX;

    private Topics() {}
}
