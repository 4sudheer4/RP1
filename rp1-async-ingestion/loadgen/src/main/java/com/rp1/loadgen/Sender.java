package com.rp1.loadgen;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.rp1.common.EnvelopeFactory;
import com.rp1.common.Json;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SFMC ENS-style sender. Open loop: batches are generated at a fixed rate regardless of how the server is doing,
 * because SFMC does not slow down for us. Each batch gets up to 1 + retries attempts (ENS: "retries up to three
 * times", then drops the batch), each with a hard client timeout (ENS: 2s). Retries resend the identical bytes.
 * The retry backoff is not documented by SFMC; it is a parameter here.
 *
 * <p>Optional ENS suspension model (off by default): N consecutive failed deliveries suspend the callback for a
 * window, and batches arising during suspension are dropped, as the official docs describe.
 *
 * <p>Writes a ledger (every generated event id) so the verifier can diff against what was persisted.
 */
final class Sender {

    record Config(String url, double rate, int durationS, int batchSize, Duration timeout, int retries,
                  long[] retryDelaysMs, int subscribers, int activeMessages, double hotFraction,
                  double invalidFraction, int poisonCount, String signingKey, String keyEncoding, Path out,
                  String metricsUrl, int suspendAfter, long suspendMs, long seed) {}

    record Attempt(int status, long latencyMs) {}

    private final Config cfg;
    private final String runId;
    private final HttpClient http;
    private final SecretKeySpec key;
    private final ConcurrentLinkedQueue<String> batchLines = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, AtomicInteger> statusCounts = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> firstLatencies = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();
    private final AtomicLong attemptsTotal = new AtomicLong();
    private final AtomicLong firstOk = new AtomicLong(), eventualOk = new AtomicLong(), dropped = new AtomicLong(),
            suspendedDrops = new AtomicLong();
    // ENS suspension model
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private volatile long suspendedUntilMs;
    private final AtomicLong suspensions = new AtomicLong();

    Sender(Config cfg) {
        this.cfg = cfg;
        this.runId = "r" + Long.toString(System.currentTimeMillis(), 36);
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(1)).build();
        byte[] k = "BASE64".equalsIgnoreCase(cfg.keyEncoding()) ? Base64.getDecoder().decode(cfg.signingKey())
                : cfg.signingKey().getBytes(StandardCharsets.UTF_8);
        this.key = new SecretKeySpec(k, "HmacSHA256");
    }

    Map<String, Object> run() throws Exception {
        Files.createDirectories(cfg.out());
        String metricsBefore = scrape();
        EventGenerator gen = new EventGenerator(cfg.seed(), runId, cfg.subscribers(), cfg.activeMessages(),
                cfg.hotFraction(), cfg.invalidFraction(), cfg.poisonCount());
        long totalBatches = Math.round(cfg.rate() * cfg.durationS());
        long periodNanos = (long) (1e9 / cfg.rate());
        AtomicLong seq = new AtomicLong();
        Instant started = Instant.now();

        try (BufferedWriter events = Files.newBufferedWriter(cfg.out().resolve("events.csv"));
             ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor()) {
            events.write("event_id,batch_id,valid,message_key,event_type,rank,status,poison\n");
            ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
            ticker.scheduleAtFixedRate(() -> {
                long n = seq.getAndIncrement();
                if (n >= totalBatches) return;
                String batchId = runId + "-" + n;
                byte[] body = buildBatch(gen, batchId, events);
                senders.submit(() -> deliver(batchId, body));
            }, 0, periodNanos, TimeUnit.NANOSECONDS);
            while (seq.get() < totalBatches) Thread.sleep(50);
            ticker.shutdown();
            ticker.awaitTermination(5, TimeUnit.SECONDS);
            senders.shutdown();
            long maxDrain = cfg.timeout().toMillis() * (cfg.retries() + 1) + Arrays.stream(cfg.retryDelaysMs()).sum() + 10_000;
            senders.awaitTermination(maxDrain, TimeUnit.MILLISECONDS);
        }
        Instant finished = Instant.now();
        Files.write(cfg.out().resolve("batches.csv"), (Iterable<String>) () -> {
            List<String> lines = new ArrayList<>();
            lines.add("batch_id,events,attempts,first_status,final_status,first_latency_ms,outcome");
            lines.addAll(batchLines);
            return lines.iterator();
        });
        String metricsAfter = scrape();
        if (metricsBefore != null) Files.writeString(cfg.out().resolve("metrics-before.prom"), metricsBefore);
        if (metricsAfter != null) Files.writeString(cfg.out().resolve("metrics-after.prom"), metricsAfter);
        return summary(started, finished, totalBatches, metricsBefore, metricsAfter);
    }

    private byte[] buildBatch(EventGenerator gen, String batchId, BufferedWriter ledger) {
        ArrayNode arr = Json.MAPPER.createArrayNode();
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < cfg.batchSize(); i++) {
            EventGenerator.Generated g = gen.next();
            arr.add(g.event());
            String id = EnvelopeFactory.eventId(g.event());
            rows.append(id).append(',').append(batchId).append(',').append(g.valid()).append(',')
                    .append(g.messageKey() == null ? "" : g.messageKey()).append(',').append(g.type().wireName()).append(',')
                    .append(g.type().rank()).append(',').append(g.type().status() == null ? "" : g.type().status()).append(',')
                    .append(g.subscriberKey() != null && g.subscriberKey().startsWith("poison-")).append('\n');
        }
        try {
            ledger.write(rows.toString()); // single ticker thread: no synchronization needed
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return arr.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void deliver(String batchId, byte[] body) {
        if (cfg.suspendAfter() > 0 && System.currentTimeMillis() < suspendedUntilMs) {
            suspendedDrops.incrementAndGet();
            dropped.incrementAndGet();
            batchLines.add(batchId + "," + cfg.batchSize() + ",0,suspended,suspended,0,dropped_suspended");
            return;
        }
        String sig = sign(body);
        List<Attempt> attempts = new ArrayList<>();
        boolean ok = false;
        for (int a = 0; a <= cfg.retries(); a++) {
            Attempt at = post(batchId, body, sig);
            attempts.add(at);
            attemptsTotal.incrementAndGet();
            allLatencies.add(at.latencyMs());
            statusCounts.computeIfAbsent((a == 0 ? "first:" : "retry:") + label(at.status()), k -> new AtomicInteger()).incrementAndGet();
            if (at.status() >= 200 && at.status() <= 204) {
                ok = true;
                consecutiveFailures.set(0);
                break;
            }
            onDeliveryFailure();
            if (a < cfg.retries()) sleep(cfg.retryDelaysMs()[Math.min(a, cfg.retryDelaysMs().length - 1)]);
        }
        Attempt first = attempts.get(0);
        firstLatencies.add(first.latencyMs());
        if (ok && attempts.size() == 1) firstOk.incrementAndGet();
        if (ok) eventualOk.incrementAndGet(); else dropped.incrementAndGet();
        batchLines.add(batchId + "," + cfg.batchSize() + "," + attempts.size() + "," + label(first.status()) + ","
                + label(attempts.get(attempts.size() - 1).status()) + "," + first.latencyMs() + ","
                + (ok ? (attempts.size() == 1 ? "accepted_first" : "accepted_retry") : "dropped"));
    }

    private void onDeliveryFailure() {
        if (cfg.suspendAfter() <= 0) return;
        if (consecutiveFailures.incrementAndGet() >= cfg.suspendAfter()) {
            consecutiveFailures.set(0);
            suspendedUntilMs = System.currentTimeMillis() + cfg.suspendMs();
            suspensions.incrementAndGet();
        }
    }

    private Attempt post(String batchId, byte[] body, String sig) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(cfg.url())).timeout(cfg.timeout())
                .header("Content-Type", "application/json")
                .header("x-sfmc-ens-signature", sig)
                .header("X-Request-Id", batchId)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        long t0 = System.nanoTime();
        int status;
        try {
            status = http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (HttpTimeoutException e) {
            status = -1; // client timeout: SFMC counts this as a failure and retries
        } catch (IOException e) {
            status = -2; // connection refused/reset
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            status = -2;
        }
        return new Attempt(status, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
    }

    private static String label(int status) {
        return switch (status) {
            case -1 -> "timeout";
            case -2 -> "conn_error";
            default -> Integer.toString(status);
        };
    }

    private String sign(byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(key);
            return Base64.getEncoder().encodeToString(mac.doFinal(body));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String scrape() {
        if (cfg.metricsUrl() == null) return null;
        try {
            return http.send(HttpRequest.newBuilder(URI.create(cfg.metricsUrl())).timeout(Duration.ofSeconds(5)).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
        } catch (Exception e) {
            return null;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Map<String, Object> summary(Instant started, Instant finished, long batches, String before, String after) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("runId", runId);
        s.put("startedAt", started.toString());
        s.put("finishedAt", finished.toString());
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("url", cfg.url());
        c.put("rateBatchesPerSec", cfg.rate());
        c.put("durationS", cfg.durationS());
        c.put("batchSize", cfg.batchSize());
        c.put("clientTimeoutMs", cfg.timeout().toMillis());
        c.put("retries", cfg.retries());
        c.put("retryDelaysMs", cfg.retryDelaysMs());
        c.put("hotFraction", cfg.hotFraction());
        c.put("invalidFraction", cfg.invalidFraction());
        c.put("poisonCount", cfg.poisonCount());
        c.put("suspendAfter", cfg.suspendAfter());
        c.put("suspendMs", cfg.suspendMs());
        s.put("config", c);
        s.put("batches", batches);
        s.put("events", batches * cfg.batchSize());
        s.put("batchCaptureFirstAttempt", ratio(firstOk.get(), batches));
        s.put("batchCaptureEventual", ratio(eventualOk.get(), batches));
        s.put("batchesDropped", dropped.get());
        s.put("batchesDroppedBySuspension", suspendedDrops.get());
        s.put("suspensions", suspensions.get());
        s.put("httpAttempts", attemptsTotal.get());
        s.put("clientAttemptsPerBatch", ratio(attemptsTotal.get(), batches));
        Map<String, Integer> sc = new java.util.TreeMap<>();
        statusCounts.forEach((k, v) -> sc.put(k, v.get()));
        s.put("statusCounts", sc);
        s.put("firstAttemptLatencyMs", Stats.percentiles(firstLatencies));
        s.put("allAttemptsLatencyMs", Stats.percentiles(allLatencies));
        if (before != null && after != null) s.put("serverMetricsDelta", Stats.promDelta(before, after, "rp1_ingest_"));
        return s;
    }

    private static double ratio(long a, long b) {
        return b == 0 ? 0 : Math.round(1_000_000.0 * a / b) / 1_000_000.0;
    }
}
