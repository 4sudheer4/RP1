package com.rp1.common.errors;

/**
 * Transient failure: the same input may succeed later (downstream 5xx/429/timeout, DB connection loss).
 * Consumer routes to non-blocking retry topics with backoff; exhausted retries go to the DLT.
 */
public class RetryableException extends RuntimeException {
    public RetryableException(String message) { super(message); }
    public RetryableException(String message, Throwable cause) { super(message, cause); }
}
