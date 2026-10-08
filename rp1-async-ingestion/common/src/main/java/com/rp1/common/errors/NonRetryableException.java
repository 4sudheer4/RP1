package com.rp1.common.errors;

/**
 * Permanent failure: retrying the same input cannot succeed (schema violation, downstream 4xx other than 429,
 * constraint violation that is not the dedup key). Consumer sends straight to the DLT, skipping retry topics.
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) { super(message); }
    public NonRetryableException(String message, Throwable cause) { super(message, cause); }
}
