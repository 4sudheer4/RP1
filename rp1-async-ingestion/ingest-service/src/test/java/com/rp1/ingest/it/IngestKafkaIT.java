package com.rp1.ingest.it;

import com.rp1.common.Json;
import com.rp1.ingest.IngestApplication;
import com.rp1.ingest.TestBatches;
import com.rp1.ingest.engine.SignatureVerifier;
import com.rp1.testkit.KafkaBehindToxiproxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real broker, real producer config, Kafka reachable only through Toxiproxy. Each test boots its own app context
 * so shutdown can be tested and breaker state does not leak between tests.
 */
class IngestKafkaIT {
    static final Duration DEADLINE = Duration.ofMillis(1500);
    static final Duration SLACK = Duration.ofMillis(400); // HTTP + scheduling overhead on a laptop

    final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    ConfigurableApplicationContext app;
    String url;

    @BeforeAll
    static void infra() throws Exception {
        KafkaBehindToxiproxy.start(Map.of("callback-events", 12, "invalid-events", 3));
    }

    @BeforeEach
    void boot() throws Exception {
        KafkaBehindToxiproxy.resetToxics();
        app = new SpringApplicationBuilder(IngestApplication.class).profiles("textlogs").run(
                "--server.port=0",
                "--spring.kafka.bootstrap-servers=" + KafkaBehindToxiproxy.proxiedBootstrap(),
                "--rp1.ingest.signature.keys=" + TestBatches.KEY,
                "--rp1.ingest.breaker.wait-in-open-state=1s",
                "--rp1.ingest.breaker.minimum-number-of-calls=4",
                "--rp1.ingest.breaker.sliding-window-size=4");
        url = "http://localhost:" + app.getEnvironment().getProperty("local.server.port") + "/callbacks";
        await().atMost(Duration.ofSeconds(30)).until(() -> health("readiness").contains("\"UP\""));
    }

    @AfterEach
    void close() throws Exception {
        if (app != null && app.isActive()) app.close();
        KafkaBehindToxiproxy.resetToxics();
    }

    private String health(String group) throws Exception {
        var req = HttpRequest.newBuilder(URI.create(url.replace("/callbacks", "/actuator/health/" + group))).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }

    record Result(int status, String body, Duration elapsed) {}

    private Result post(String body) throws Exception {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        var req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .header(SignatureVerifier.HEADER, SignatureVerifier.sign(b, TestBatches.KEY.getBytes(StandardCharsets.UTF_8)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(b)).build();
        long t0 = System.nanoTime();
        var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new Result(resp.statusCode(), resp.body(), Duration.ofNanos(System.nanoTime() - t0));
    }

    @Test
    void acceptedBatchIsDurablyInKafkaKeyedBySubscriberWithInvalidRouted() throws Exception {
        String batchBody = "[" + TestBatches.event("EmailSent", "it1-sub-a", "it1-m1", 1) + ","
                + TestBatches.event("EmailBounced", "it1-sub-a", "it1-m1", 2) + ","
                + TestBatches.event("EmailSent", "it1-sub-b", "it1-m2", 3) + ","
                + "{\"eventCategoryType\":\"Nope.Nope\",\"info\":{\"subscriberKey\":\"it1-x\"}}]";
        var r = post(batchBody);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.elapsed()).isLessThan(DEADLINE);

        List<ConsumerRecord<String, byte[]>> events = KafkaBehindToxiproxy.readAll("callback-events").stream()
                .filter(c -> c.key().startsWith("it1-")).toList();
        assertThat(events).hasSize(3);
        // per-subscriber ordering: both events of sub-a on one partition, in send order
        Map<String, List<ConsumerRecord<String, byte[]>>> byKey = events.stream().collect(Collectors.groupingBy(ConsumerRecord::key));
        var subA = byKey.get("it1-sub-a");
        assertThat(subA).extracting(ConsumerRecord::partition).containsOnly(subA.get(0).partition());
        assertThat(Json.MAPPER.readTree(subA.get(0).value()).get("eventType").asText()).endsWith("EmailSent");
        assertThat(Json.MAPPER.readTree(subA.get(1).value()).get("eventType").asText()).endsWith("EmailBounced");

        var invalid = KafkaBehindToxiproxy.readAll("invalid-events").stream()
                .filter(c -> new String(c.value(), StandardCharsets.UTF_8).contains("it1-x")).toList();
        assertThat(invalid).hasSize(1);
        assertThat(Json.MAPPER.readTree(invalid.get(0).value()).get("reason").asText()).isEqualTo("UNSUPPORTED_EVENT_TYPE");
    }

    @Test
    void slowKafkaDeadlineHeldNoPileUpThenBreakerFailsFastAndRecovers() throws Exception {
        KafkaBehindToxiproxy.proxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 3000);

        var first = post(TestBatches.batch(5, "slow1"));
        assertThat(first.status()).isEqualTo(503);
        assertThat(first.elapsed()).isLessThan(DEADLINE.plus(SLACK));

        // burst of concurrent requests: every one answers inside the deadline (timeout, breaker or bulkhead)
        ExecutorService pool = Executors.newFixedThreadPool(40);
        List<Future<Result>> futures = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            int n = i;
            futures.add(pool.submit(() -> post(TestBatches.batch(3, "burst" + n))));
        }
        List<Result> results = new ArrayList<>();
        for (Future<Result> f : futures) results.add(f.get());
        pool.shutdown();
        assertThat(results).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(503);
            assertThat(r.elapsed()).isLessThan(DEADLINE.plus(SLACK));
        });
        assertThat(results).anySatisfy(r -> assertThat(r.body()).contains("breaker_open"));
        var open = results.stream().filter(r -> r.body().contains("breaker_open")).toList();
        assertThat(open).allSatisfy(r -> assertThat(r.elapsed()).isLessThan(Duration.ofMillis(300)));

        KafkaBehindToxiproxy.resetToxics();
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(300))
                .until(() -> post(TestBatches.batch(2, "recover")).status() == 200);
    }

    @Test
    void kafkaCutReturns503WithinDeadlineNeverHangs() throws Exception {
        KafkaBehindToxiproxy.proxy.disable();
        for (int i = 0; i < 3; i++) {
            var r = post(TestBatches.batch(4, "cut" + i));
            assertThat(r.status()).isEqualTo(503);
            assertThat(r.elapsed()).isLessThan(DEADLINE.plus(SLACK));
        }
    }

    @Test
    void gracefulShutdownDrainsInFlightRequestAndItsEventIsInKafka() throws Exception {
        // 400ms per hop: the request is mid-produce when shutdown starts
        KafkaBehindToxiproxy.proxy.toxics().latency("drain", ToxicDirection.DOWNSTREAM, 400);
        CompletableFuture<Result> inFlight = CompletableFuture.supplyAsync(() -> {
            try {
                return post("[" + TestBatches.event("EmailSent", "drain-sub", "drain-m", 1) + "]");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(150);
        CompletableFuture<Void> closing = CompletableFuture.runAsync(app::close);
        Result r = inFlight.get();
        closing.get();
        assertThat(r.status()).isEqualTo(200);
        KafkaBehindToxiproxy.resetToxics();
        assertThat(KafkaBehindToxiproxy.readAll("callback-events")).anySatisfy(c -> assertThat(c.key()).isEqualTo("drain-sub"));
    }
}
