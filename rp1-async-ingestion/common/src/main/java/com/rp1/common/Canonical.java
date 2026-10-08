package com.rp1.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;

/**
 * Canonical JSON and the content-hash event id.
 *
 * <p>SFMC ENS events carry no documented unique event id ({@code compositeId} is an "internal tracking ID
 * (deprecated)" and is not documented as unique per event), so the dedup key is SHA-256 over a canonical form of
 * the event. An explicit upstream id ({@code eventId}) is preferred when a source provides one:
 * object keys sorted recursively, no insignificant whitespace, array order preserved. Two byte-different
 * serializations of the same event (key order, spacing) get the same id; SFMC retries resend the same event, so
 * they dedup. Limitation: numbers are compared as Jackson parses them, so {@code 1} and {@code 1.0} differ.
 */
public final class Canonical {
    private static final ObjectMapper CANON = JsonMapper.builder()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();

    private Canonical() {}

    public static byte[] bytes(JsonNode node) {
        try {
            return CANON.writeValueAsBytes(sorted(node));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("canonicalization failed", e);
        }
    }

    /** Hex SHA-256 of the canonical form: the event id used as the idempotency key end to end. */
    public static String eventId(JsonNode event) {
        return sha256Hex(bytes(event));
    }

    public static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String sha256Hex(String s) {
        return sha256Hex(s.getBytes(StandardCharsets.UTF_8));
    }

    private static JsonNode sorted(JsonNode node) {
        if (node.isObject()) {
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(null);
            ObjectNode out = JsonNodeFactory.instance.objectNode();
            for (String n : names) out.set(n, sorted(node.get(n)));
            return out;
        }
        if (node.isArray()) {
            ArrayNode out = JsonNodeFactory.instance.arrayNode();
            for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) out.add(sorted(it.next()));
            return out;
        }
        return node;
    }

    /** Short, non-reversible tag for logging identifiers that may be PII (subscriberKey is often an email). */
    public static String redact(String value) {
        if (value == null) return "null";
        return "h:" + sha256Hex(value).substring(0, 12);
    }
}
