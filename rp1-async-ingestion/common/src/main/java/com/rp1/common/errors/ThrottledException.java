package com.rp1.common.errors;

/** Downstream signalled rate limiting (HTTP 429 equivalent). Retryable, and also a cue to pause consumption. */
public class ThrottledException extends RetryableException {
    public ThrottledException(String message) { super(message); }
}
