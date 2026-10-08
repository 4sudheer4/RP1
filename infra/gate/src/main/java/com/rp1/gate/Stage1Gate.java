package com.rp1.gate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stage-1 gate client.
 *
 * produce: sends N records (acks=all) at a fixed rate, writes a ledger of acked/failed ids, prints latency + errors.
 * verify:  reads the whole topic and diffs it against every ledger in a directory.
 *
 * Exit code is the gate signal: produce exits non-zero when the outcome contradicts --expect (all-acked | all-failed | any).
 */
public final class Stage1Gate {

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parse(args);
        switch (args[0]) {
            case "produce" -> System.exit(produce(a));
            case "verify" -> System.exit(verify(a));
            default -> throw new IllegalArgumentException("usage: produce|verify --key value ...");
        }
    }

    // ---------------------------------------------------------------- produce

    static int produce(Map<String, String> a) throws Exception {
        String topic = a.get("topic");
        String phase = a.get("phase");
        int count = Integer.parseInt(a.getOrDefault("count", "1000"));
        int rate = Integer.parseInt(a.getOrDefault("rate", "0"));          // msgs/s, 0 = unthrottled
        String expect = a.getOrDefault("expect", "all-acked");
        // App-side bound on waiting for callbacks. Needed because delivery.timeout.ms does NOT bound a fresh
        // idempotent producer that cannot obtain a producer id (e.g. KRaft quorum lost): records never expire.
        long deadlineMs = Long.parseLong(a.getOrDefault("deadline-ms", "0"));   // 0 = wait forever
        boolean idempotent = !a.containsKey("no-idempotence");
        Path ledgerDir = Path.of(a.get("ledger"));
        Files.createDirectories(ledgerDir);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, a.getOrDefault("bootstrap", "localhost:39092,localhost:39093,localhost:39094"));
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotent));
        p.put(ProducerConfig.RETRIES_CONFIG, a.getOrDefault("retries", String.valueOf(Integer.MAX_VALUE)));
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, a.getOrDefault("request-timeout-ms", "10000"));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, a.getOrDefault("delivery-timeout-ms", "60000"));
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, a.getOrDefault("max-block-ms", "10000"));
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.printf("[produce] phase=%s topic=%s count=%d rate=%s expect=%s acks=all idempotence=%s retries=%s request.timeout.ms=%s delivery.timeout.ms=%s bootstrap=%s%n",
                phase, topic, count, rate == 0 ? "max" : rate + "/s", expect, idempotent, p.get(ProducerConfig.RETRIES_CONFIG),
                p.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), p.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), p.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        String runId = a.get("run");
        Queue<String> acked = new ConcurrentLinkedQueue<>();
        Queue<String> failed = new ConcurrentLinkedQueue<>();
        Queue<Long> latMicros = new ConcurrentLinkedQueue<>();
        Map<String, Integer> errors = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(count);
        AtomicLong lastAckEpochMs = new AtomicLong(0);
        boolean deadlineHit = false;

        long start = System.nanoTime();
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        try {
            for (int i = 0; i < count; i++) {
                if (rate > 0) {
                    long due = start + (long) i * 1_000_000_000L / rate;
                    long wait = due - System.nanoTime();
                    if (wait > 0) Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
                String id = runId + ":" + phase + ":" + i;
                String key = "sub-" + (i % 64);   // per-subscriber key, spreads across all partitions
                long t0 = System.nanoTime();
                try {
                    producer.send(new ProducerRecord<>(topic, key, id), (md, ex) -> {
                        latMicros.add((System.nanoTime() - t0) / 1000);
                        if (ex == null) { acked.add(id); lastAckEpochMs.accumulateAndGet(System.currentTimeMillis(), Math::max); }
                        else { failed.add(id); errors.merge(ex.getClass().getSimpleName() + ": " + firstLine(ex.getMessage()), 1, Integer::sum); }
                        done.countDown();
                    });
                } catch (Exception ex) {   // send() itself can throw (e.g. metadata wait exceeds max.block.ms)
                    failed.add(id);
                    errors.merge("send() threw " + ex.getClass().getSimpleName() + ": " + firstLine(ex.getMessage()), 1, Integer::sum);
                    done.countDown();
                }
            }
            if (deadlineMs > 0) {
                long remaining = deadlineMs - (System.nanoTime() - start) / 1_000_000;
                deadlineHit = !done.await(Math.max(remaining, 0), TimeUnit.MILLISECONDS);
            } else {
                done.await();
            }
        } finally {
            // Past the deadline: force-close aborts unsent batches, so their callbacks fire as failures.
            producer.close(deadlineHit ? Duration.ZERO : Duration.ofSeconds(30));
        }
        done.await(5, TimeUnit.SECONDS);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Files.write(ledgerDir.resolve(phase + ".acked"), acked);
        Files.write(ledgerDir.resolve(phase + ".failed"), failed);

        long[] lat = latMicros.stream().mapToLong(Long::longValue).sorted().toArray();
        System.out.printf("[produce] phase=%s acked=%d failed=%d elapsed=%dms ack-latency p50=%.1fms p99=%.1fms max=%.1fms%n",
                phase, acked.size(), failed.size(), elapsedMs, pct(lat, 50), pct(lat, 99), pct(lat, 100));
        if (deadlineHit) System.out.printf("[produce]   app deadline %dms hit before producer resolved all sends -> force-closed%n", deadlineMs);
        if (lastAckEpochMs.get() > 0) System.out.printf("[produce]   last-ack-epoch-ms=%d%n", lastAckEpochMs.get());
        errors.forEach((k, v) -> System.out.printf("[produce]   error x%d  %s%n", v, k));

        boolean ok = switch (expect) {
            case "all-acked" -> failed.isEmpty() && acked.size() == count;
            case "all-failed" -> acked.isEmpty() && failed.size() == count;
            // Caller judges (e.g. "no ack after kill time" using last-ack-epoch-ms); only require every send resolved.
            case "any" -> acked.size() + failed.size() == count;
            default -> throw new IllegalArgumentException("expect must be all-acked|all-failed|any");
        };
        System.out.printf("[produce] phase=%s expect=%s -> %s%n", phase, expect, ok ? "PASS" : "FAIL");
        return ok ? 0 : 1;
    }

    // ---------------------------------------------------------------- verify

    /** Every acked id must be in the log (zero loss). Failed ids may or may not be present: reported, not judged. */
    static int verify(Map<String, String> a) throws IOException {
        String topic = a.get("topic");
        String runId = a.get("run");
        Path ledgerDir = Path.of(a.get("ledger"));

        Set<String> acked = readLedger(ledgerDir, ".acked");
        Set<String> failed = readLedger(ledgerDir, ".failed");

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, a.getOrDefault("bootstrap", "localhost:49092,localhost:49093,localhost:49094"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Map<String, Integer> seen = new HashMap<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream()
                    .map(pi -> new TopicPartition(topic, pi.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            while (tps.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofSeconds(2))) {
                    if (r.value().startsWith(runId + ":")) seen.merge(r.value(), 1, Integer::sum);
                }
            }
        }

        long missingAcked = acked.stream().filter(id -> !seen.containsKey(id)).count();
        long failedButPresent = failed.stream().filter(seen::containsKey).count();
        long duplicates = seen.values().stream().filter(n -> n > 1).count();
        System.out.printf("[verify] topic=%s run=%s ledger: acked=%d failed=%d | log: distinct=%d duplicate-ids=%d%n",
                topic, runId, acked.size(), failed.size(), seen.size(), duplicates);
        System.out.printf("[verify] acked-but-missing (LOSS)=%d   failed-but-present (ambiguous failure, retry would duplicate)=%d%n",
                missingAcked, failedButPresent);
        boolean ok = missingAcked == 0 && !acked.isEmpty();
        System.out.printf("[verify] zero loss of acked writes -> %s%n", ok ? "PASS" : "FAIL");
        return ok ? 0 : 1;
    }

    // ---------------------------------------------------------------- helpers

    static Set<String> readLedger(Path dir, String suffix) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            Set<String> ids = new HashSet<>();
            for (Path f : files.filter(f -> f.toString().endsWith(suffix)).collect(Collectors.toList())) {
                ids.addAll(Files.readAllLines(f));
            }
            return ids;
        }
    }

    static double pct(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))] / 1000.0;
    }

    static String firstLine(String s) {
        return s == null ? "" : s.lines().findFirst().orElse("");
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            String k = args[i].replaceFirst("^--", "");
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) m.put(k, args[++i]);
            else m.put(k, "true");
        }
        return m;
    }
}
