package com.rp1.ingest.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EnvelopeFactory;
import com.rp1.common.Json;
import com.rp1.ingest.config.IngestProperties;
import com.rp1.ingest.engine.BodyReader;
import com.rp1.ingest.engine.IngestPath;
import com.rp1.ingest.engine.SignatureVerifier;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * POST /callbacks. Order matters and is cheapest-first, all before any produce:
 * admission (bulkhead) -> media type (415) -> bounded body read (413) -> auth / ENS handshake (401) ->
 * JSON array parse (400) -> batch size (413) -> per-event validation (invalid events routed, not rejected) ->
 * {@link IngestPath} (200 only when durably accepted, else fast 503 with Retry-After).
 *
 * <p>No payloads are logged: only ids, counts, redacted keys, outcome and latency, with traceId in MDC.
 */
@RestController
public class CallbackController {
    private static final Logger log = LoggerFactory.getLogger(CallbackController.class);
    /** Separate logger so the one-time ENS verification details can be routed/kept apart from request logs. */
    private static final Logger verificationLog = LoggerFactory.getLogger("rp1.ens.verification");
    private static final Pattern TRACEPARENT = Pattern.compile("^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$");
    private static final Pattern SAFE_ID = Pattern.compile("^[A-Za-z0-9._:-]{1,64}$");

    private final IngestProperties props;
    private final IngestPath path;
    private final @Nullable SignatureVerifier verifier;
    private final EnvelopeFactory envelopes;
    private final Semaphore inFlight;
    private final long deadlineNanos;
    private final MeterRegistry registry;
    private final Timer.Builder ackLatency;
    private final String mode;

    public CallbackController(IngestProperties props, IngestPath path, @Nullable SignatureVerifier verifier, MeterRegistry registry) {
        this.props = props;
        this.path = path;
        this.verifier = verifier;
        this.envelopes = new EnvelopeFactory(props.maxClockSkew());
        this.inFlight = new Semaphore(props.maxInFlight());
        this.deadlineNanos = props.hardDeadline().toNanos();
        this.registry = registry;
        this.mode = props.mode().name().toLowerCase();
        this.ackLatency = Timer.builder("rp1.ingest.ack.latency")
                .description("time from handler start to response, by mode/status/cause")
                .publishPercentileHistogram()
                .serviceLevelObjectives(Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
                        Duration.ofMillis(500), Duration.ofMillis(1000), Duration.ofMillis(1500), Duration.ofMillis(2000))
                .tag("mode", mode);
        Gauge.builder("rp1.ingest.inflight", () -> props.maxInFlight() - inFlight.availablePermits()).register(registry);
    }

    @PostMapping("/callbacks")
    public ResponseEntity<Map<String, Object>> callbacks(HttpServletRequest request) {
        long start = System.nanoTime();
        Instant receivedAt = Instant.now();
        String traceId = traceId(request.getHeader("traceparent"));
        String batchId = batchId(request.getHeader("X-Request-Id"));
        MDC.put("traceId", traceId);
        MDC.put("batchId", batchId);
        try {
            if (!inFlight.tryAcquire()) return respond(start, 503, "bulkhead_full", null);
            try {
                return handle(request, start, receivedAt, traceId, batchId);
            } finally {
                inFlight.release();
            }
        } finally {
            MDC.clear();
        }
    }

    private ResponseEntity<Map<String, Object>> handle(HttpServletRequest request, long start, Instant receivedAt,
                                                       String traceId, String batchId) {
        if (!isJson(request.getContentType())) return respond(start, 415, "unsupported_media_type", null);

        byte[] body;
        try {
            body = BodyReader.read(request.getInputStream(), request.getContentLengthLong(), props.maxBodyBytes());
        } catch (BodyReader.TooLarge e) {
            return respond(start, 413, "body_too_large", null);
        } catch (IOException e) {
            return respond(start, 400, "body_read_error", null);
        }

        String signature = request.getHeader(SignatureVerifier.HEADER);
        if (verifier != null) {
            if (signature == null) return handshakeOrReject(start, body);
            int key = verifier.verify(body, signature);
            if (key == SignatureVerifier.NO_MATCH) return respond(start, 401, "bad_signature", null);
            Counter.builder("rp1.ingest.signature").tag("key", key == 0 ? "current" : "previous").register(registry).increment();
        }

        JsonNode root;
        try {
            root = Json.MAPPER.readTree(body);
        } catch (IOException e) {
            return respond(start, 400, "malformed_json", null);
        }
        if (root == null || !root.isArray()) {
            if (verifier == null && isHandshake(root)) return handshake(start, root);
            return respond(start, 400, "expected_json_array", null);
        }
        if (root.size() > props.maxBatchEvents()) return respond(start, 413, "too_many_events", null);
        if (root.isEmpty()) return respond(start, 200, "empty_batch", Map.of("events", 0));

        List<CallbackEnvelope> valid = new ArrayList<>(root.size());
        List<EnvelopeFactory.Invalid> invalid = new ArrayList<>();
        for (JsonNode event : root) {
            switch (envelopes.build(event, batchId, receivedAt, traceId)) {
                case EnvelopeFactory.Valid v -> valid.add(v.envelope());
                case EnvelopeFactory.Invalid i -> invalid.add(i);
            }
        }
        countEvents(valid, invalid);

        var outcome = path.ingest(new IngestPath.Batch(batchId, traceId, receivedAt, start, valid, invalid), start + deadlineNanos);
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("events", valid.size());
        details.put("invalid", invalid.size());
        return respond(start, outcome.status, outcome.tag(), details);
    }

    private ResponseEntity<Map<String, Object>> handshakeOrReject(long start, byte[] body) {
        try {
            JsonNode root = Json.MAPPER.readTree(body);
            if (isHandshake(root)) return handshake(start, root);
        } catch (IOException ignored) {
            // fall through: unsigned and not a handshake
        }
        return respond(start, 401, "missing_signature", null);
    }

    /**
     * ENS two-step verification (official POST /platform/v1/ens-verify docs): ENS posts {callbackId,
     * verificationKey} to the callback; we must return 200 and get those values to whoever calls ens-verify.
     * The docs' suggested approach is logging them for manual verification; the key is single-use and only
     * completes registration, so logging it is the documented trade-off. Accepted only while enabled.
     */
    private ResponseEntity<Map<String, Object>> handshake(long start, JsonNode root) {
        IngestProperties.Verification v = props.verification();
        if (v == null || !v.enabled()) return respond(start, 401, "verification_disabled", null);
        String callbackId = root.get("callbackId").asText();
        if (v.expectedCallbackId() != null && !v.expectedCallbackId().isBlank() && !v.expectedCallbackId().equals(callbackId)) {
            return respond(start, 403, "unexpected_callback_id", null);
        }
        verificationLog.warn("ENS callback verification received: callbackId={} verificationKey={} "
                + "-> call POST /platform/v1/ens-verify within 4h, then disable rp1.ingest.verification",
                callbackId, root.get("verificationKey").asText());
        return respond(start, 200, "verification_received", null);
    }

    private static boolean isHandshake(JsonNode root) {
        return root != null && root.isObject() && root.size() == 2
                && root.path("callbackId").isTextual() && root.path("verificationKey").isTextual();
    }

    private ResponseEntity<Map<String, Object>> respond(long start, int status, String cause, Map<String, Object> details) {
        long nanos = System.nanoTime() - start;
        ackLatency.tag("status", Integer.toString(status)).tag("cause", cause).register(registry).record(nanos, TimeUnit.NANOSECONDS);
        Counter.builder("rp1.ingest.responses").tag("mode", mode).tag("status", Integer.toString(status)).tag("cause", cause)
                .register(registry).increment();
        long ms = TimeUnit.NANOSECONDS.toMillis(nanos);
        if (status >= 500) log.warn("callback batch status={} cause={} latencyMs={} details={}", status, cause, ms, details);
        else log.info("callback batch status={} cause={} latencyMs={} details={}", status, cause, ms, details);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put(status < 300 ? "result" : "error", cause);
        if (details != null) body.putAll(details);
        var builder = ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON);
        if (status == 503) builder.header(HttpHeaders.RETRY_AFTER, Integer.toString(props.retryAfterSeconds()));
        return builder.body(body);
    }

    private void countEvents(List<CallbackEnvelope> valid, List<EnvelopeFactory.Invalid> invalid) {
        Counter.builder("rp1.ingest.events").tag("result", "valid").register(registry).increment(valid.size());
        for (EnvelopeFactory.Invalid i : invalid) {
            Counter.builder("rp1.ingest.events").tag("result", "invalid").tag("reason", i.reason().name().toLowerCase())
                    .register(registry).increment();
        }
        long skewed = valid.stream().filter(CallbackEnvelope::clockSkewed).count();
        if (skewed > 0) Counter.builder("rp1.ingest.clock_skewed_events").register(registry).increment(skewed);
    }

    private static boolean isJson(String contentType) {
        if (contentType == null) return false;
        try {
            return MediaType.APPLICATION_JSON.isCompatibleWith(MediaType.parseMediaType(contentType));
        } catch (RuntimeException e) {
            return false;
        }
    }

    static String traceId(String traceparent) {
        if (traceparent != null) {
            var m = TRACEPARENT.matcher(traceparent.trim());
            if (m.matches()) return m.group(1);
        }
        byte[] b = new byte[16];
        ThreadLocalRandom.current().nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    private static String batchId(String requestId) {
        return requestId != null && SAFE_ID.matcher(requestId).matches() ? requestId : UUID.randomUUID().toString();
    }
}
