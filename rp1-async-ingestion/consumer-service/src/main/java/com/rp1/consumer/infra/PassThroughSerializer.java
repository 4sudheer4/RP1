package com.rp1.consumer.infra;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rp1.common.Json;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Producer value serializer for retry/DLT publishing. Records that deserialized fine are re-published as the same
 * envelope JSON; records that failed deserialization arrive as their original raw bytes (Spring restores them
 * from the exception header) and are passed through untouched, so the DLT keeps the exact poison bytes.
 */
public class PassThroughSerializer implements Serializer<Object> {
    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) return null;
        if (data instanceof byte[] raw) return raw;
        try {
            return Json.MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }
}
