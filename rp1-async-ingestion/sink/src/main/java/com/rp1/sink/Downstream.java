package com.rp1.sink;

import com.rp1.common.CallbackEnvelope;

/**
 * The external side effect per event. Implementations must be idempotent on {@code eventId}: the sink calls it
 * inside the processing txn, so a crash between call and commit repeats the call on redelivery.
 */
public interface Downstream {
    /** @throws com.rp1.common.errors.RetryableException / NonRetryableException / ThrottledException */
    void call(CallbackEnvelope envelope);
}
