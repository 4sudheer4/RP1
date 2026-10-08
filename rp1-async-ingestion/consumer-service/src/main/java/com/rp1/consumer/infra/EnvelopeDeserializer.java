package com.rp1.consumer.infra;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.Json;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * JSON -> envelope. Wrapped by Spring's ErrorHandlingDeserializer, so a failure here does not crash the poll loop
 * or block the partition: the record reaches the error handler as a DeserializationException (non-retryable)
 * and goes straight to the DLT with its raw bytes preserved.
 */
public class EnvelopeDeserializer implements Deserializer<CallbackEnvelope> {
    @Override
    public CallbackEnvelope deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            CallbackEnvelope e = Json.MAPPER.readValue(data, CallbackEnvelope.class);
            if (e.eventId() == null || e.subscriberKey() == null || e.eventType() == null || e.event() == null) {
                throw new SerializationException("envelope missing required fields");
            }
            return e;
        } catch (IOException ex) {
            throw new SerializationException("envelope is not valid JSON: " + ex.getClass().getSimpleName());
        }
    }
}
