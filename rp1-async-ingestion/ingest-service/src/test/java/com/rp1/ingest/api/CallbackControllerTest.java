package com.rp1.ingest.api;

import com.rp1.common.EnvelopeFactory;
import com.rp1.ingest.TestBatches;
import com.rp1.ingest.config.IngestProperties;
import com.rp1.ingest.engine.IngestPath;
import com.rp1.ingest.engine.SignatureVerifier;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.rp1.ingest.TestBatches.KEY;
import static com.rp1.ingest.TestBatches.OLD_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Every ingest edge case that is decided before the produce path. Produce behavior: KafkaIngestPathTest + ITs. */
class CallbackControllerTest {

    /** Records batches; outcome and optional blocking are configurable per test. */
    static class FakePath implements IngestPath {
        final List<Batch> batches = new ArrayList<>();
        volatile Outcome outcome = Outcome.ACCEPTED;
        volatile CountDownLatch block;
        final CountDownLatch entered = new CountDownLatch(1);

        @Override
        public synchronized Outcome ingest(Batch batch, long deadlineNanos) {
            batches.add(batch);
            entered.countDown();
            if (block != null) {
                try { block.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            return outcome;
        }
    }

    FakePath path;
    MockMvc mvc;

    @BeforeEach
    void setUp() {
        setUp(TestBatches.props(64, Duration.ofMillis(1500)));
    }

    void setUp(IngestProperties props) {
        path = new FakePath();
        var verifier = new SignatureVerifier(props.signature().keys(), props.signature().keyEncoding());
        mvc = MockMvcBuilders.standaloneSetup(new CallbackController(props, path, verifier, new SimpleMeterRegistry())).build();
    }

    private ResultActions postSigned(String body, String key) throws Exception {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        return mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(bytes)
                .header(SignatureVerifier.HEADER, SignatureVerifier.sign(bytes, key.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void validBatchIsAccepted() throws Exception {
        postSigned(TestBatches.batch(10, "a"), KEY).andExpect(status().isOk()).andExpect(jsonPath("$.events").value(10));
        assertThat(path.batches).hasSize(1);
        assertThat(path.batches.get(0).valid()).hasSize(10);
    }

    @Test
    void previousKeyStillAcceptedDuringRotation() throws Exception {
        postSigned(TestBatches.batch(1, "a"), OLD_KEY).andExpect(status().isOk());
    }

    @Test
    void wrongContentTypeIs415() throws Exception {
        mvc.perform(post("/callbacks").contentType(MediaType.TEXT_PLAIN).content("[]")).andExpect(status().isUnsupportedMediaType());
        mvc.perform(post("/callbacks").content("[]")).andExpect(status().isUnsupportedMediaType());
    }

    @Test
    void jsonWithCharsetIsAccepted() throws Exception {
        byte[] b = TestBatches.batch(1, "a").getBytes(StandardCharsets.UTF_8);
        mvc.perform(post("/callbacks").contentType("application/json; charset=utf-8").content(b)
                        .header(SignatureVerifier.HEADER, SignatureVerifier.sign(b, KEY.getBytes(StandardCharsets.UTF_8))))
                .andExpect(status().isOk());
    }

    @Test
    void oversizeBodyIs413BeforeAuth() throws Exception {
        String big = "[" + "\"x\",".repeat(40_000) + "\"x\"]"; // > 64 KiB test limit
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(big)).andExpect(status().isPayloadTooLarge());
        assertThat(path.batches).isEmpty();
    }

    @Test
    void tooManyEventsIs413() throws Exception {
        postSigned(TestBatches.batch(101, "a"), KEY).andExpect(status().isPayloadTooLarge())
                .andExpect(jsonPath("$.error").value("too_many_events"));
    }

    @Test
    void missingOrBadSignatureIs401AndNothingProduced() throws Exception {
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(TestBatches.batch(1, "a")))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("missing_signature"));
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(TestBatches.batch(1, "a"))
                        .header(SignatureVerifier.HEADER, SignatureVerifier.sign("other".getBytes(), KEY.getBytes())))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("bad_signature"));
        assertThat(path.batches).isEmpty();
    }

    @Test
    void malformedJsonAndNonArrayAre400() throws Exception {
        postSigned("[{\"eventCategoryType\":", KEY).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("malformed_json"));
        postSigned("{\"a\":1}", KEY).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("expected_json_array"));
    }

    @Test
    void emptyBatchIs200NoOp() throws Exception {
        postSigned("[]", KEY).andExpect(status().isOk()).andExpect(jsonPath("$.result").value("empty_batch"));
        assertThat(path.batches).isEmpty();
    }

    @Test
    void mixedBatchAcceptsValidAndRoutesInvalidInsteadOfRejecting() throws Exception {
        String body = "[" + TestBatches.event("EmailSent", "s1", "m1", 1) + ","
                + "{\"eventCategoryType\":\"Foo.Unknown\",\"info\":{\"subscriberKey\":\"s\"}}" + ","
                + "{\"eventCategoryType\":\"TransactionalSendEvents.EmailSent\",\"info\":{}}" + ","
                + "17" + "]";
        postSigned(body, KEY).andExpect(status().isOk()).andExpect(jsonPath("$.events").value(1)).andExpect(jsonPath("$.invalid").value(3));
        var batch = path.batches.get(0);
        assertThat(batch.invalid()).extracting(EnvelopeFactory.Invalid::reason).containsExactly(
                EnvelopeFactory.InvalidReason.UNSUPPORTED_EVENT_TYPE,
                EnvelopeFactory.InvalidReason.MISSING_SUBSCRIBER_KEY,
                EnvelopeFactory.InvalidReason.NOT_OBJECT);
    }

    @Test
    void duplicateBatchProducesSameEventIds() throws Exception {
        String body = TestBatches.batch(5, "dup");
        postSigned(body, KEY).andExpect(status().isOk());
        postSigned(body, KEY).andExpect(status().isOk());
        var first = path.batches.get(0).valid().stream().map(e -> e.eventId()).toList();
        var second = path.batches.get(1).valid().stream().map(e -> e.eventId()).toList();
        assertThat(second).isEqualTo(first);
        assertThat(path.batches.get(0).batchId()).isNotEqualTo(path.batches.get(1).batchId());
    }

    @Test
    void produceFailureIs503WithRetryAfter() throws Exception {
        path.outcome = IngestPath.Outcome.PRODUCE_FAILED;
        postSigned(TestBatches.batch(3, "a"), KEY).andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1")).andExpect(jsonPath("$.error").value("produce_failed"));
    }

    @Test
    void ensVerificationHandshake() throws Exception {
        String hs = "{\"callbackId\":\"cb-1\",\"verificationKey\":\"CZwJw4XATH6LK1fPWFeMDkIyVbro6T3ijXK8CrzQe2s=\"}";
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(hs))
                .andExpect(status().isOk()).andExpect(jsonPath("$.result").value("verification_received"));
        String other = hs.replace("cb-1", "cb-2");
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(other)).andExpect(status().isForbidden());
        assertThat(path.batches).isEmpty();
    }

    @Test
    void handshakeRejectedWhenVerificationDisabled() throws Exception {
        var p = TestBatches.props(64, Duration.ofMillis(1500));
        setUp(new IngestProperties(p.mode(), p.hardDeadline(), p.minProduceBudget(), p.maxBodyBytes(), p.maxBatchEvents(),
                p.maxInFlight(), p.maxClockSkew(), p.retryAfterSeconds(), p.signature(),
                new IngestProperties.Verification(false, null), p.breaker(), p.sync()));
        String hs = "{\"callbackId\":\"cb-1\",\"verificationKey\":\"k\"}";
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(hs))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("verification_disabled"));
    }

    @Test
    void traceparentIsPropagatedToEnvelope() throws Exception {
        byte[] b = TestBatches.batch(1, "a").getBytes(StandardCharsets.UTF_8);
        mvc.perform(post("/callbacks").contentType(MediaType.APPLICATION_JSON).content(b)
                .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .header(SignatureVerifier.HEADER, SignatureVerifier.sign(b, KEY.getBytes(StandardCharsets.UTF_8))))
                .andExpect(status().isOk());
        assertThat(path.batches.get(0).valid().get(0).traceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
    }

    @Test
    void bulkheadShedsWith503InsteadOfQueueing() throws Exception {
        setUp(TestBatches.props(1, Duration.ofMillis(1500)));
        path.block = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> first = pool.submit(() -> postSigned(TestBatches.batch(1, "a"), KEY).andReturn());
        assertThat(path.entered.await(5, TimeUnit.SECONDS)).isTrue();
        long t0 = System.nanoTime();
        postSigned(TestBatches.batch(1, "b"), KEY).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("bulkhead_full")).andExpect(header().exists("Retry-After"));
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(200));
        path.block.countDown();
        first.get(5, TimeUnit.SECONDS);
        pool.shutdown();
    }
}
