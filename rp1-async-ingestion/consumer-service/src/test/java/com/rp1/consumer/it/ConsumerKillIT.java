package com.rp1.consumer.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.rp1.consumer.it.ConsumerItSupport.count;
import static com.rp1.consumer.it.ConsumerItSupport.envelope;
import static com.rp1.consumer.it.ConsumerItSupport.send;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Crash mid-stream with SIGKILL (no shutdown hooks, no offset commit, txn possibly open) against the packaged jar
 * in a child JVM, then restart. Proves the exactly-once *effect*: every event exactly one row, final state
 * correct, even though some records are redelivered. Repeated downstream calls (at-least-once side effect) are
 * measured, not asserted away.
 */
class ConsumerKillIT {

    @BeforeAll
    static void infra() throws Exception {
        ConsumerItSupport.start();
    }

    private static Process launch(String group, Path log) throws Exception {
        Path jar;
        try (Stream<Path> s = Files.list(Path.of("target"))) {
            jar = s.filter(p -> p.getFileName().toString().matches("consumer-service-.*\\.jar")).findFirst()
                    .orElseThrow(() -> new IllegalStateException("run via `mvn verify` so the jar is packaged first"));
        }
        List<String> cmd = new ArrayList<>(List.of(System.getProperty("java.home") + "/bin/java", "-Xmx256m", "-jar", jar.toString(),
                "--spring.profiles.active=textlogs"));
        cmd.addAll(ConsumerItSupport.args(group, "--rp1.downstream.service-time=10ms", "--rp1.downstream.record-calls=true"));
        return new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    @Test
    void kill9MidStreamThenRestartGivesExactlyOnceEffect() throws Exception {
        String p = "kill-" + UUID.randomUUID().toString().substring(0, 8);
        int messages = 1000; // 2000 events
        for (int m = 0; m < messages; m++) {
            send(envelope("EmailSent", p + "-s" + (m % 50), p + "-m" + m, 1000));
            send(envelope("EmailBounced", p + "-s" + (m % 50), p + "-m" + m, 2000));
        }
        String group = "g-" + p;
        Path dir = Files.createDirectories(Path.of("target", "kill-it"));
        Process first = launch(group, dir.resolve("first.log"));
        await().atMost(Duration.ofSeconds(90)).ignoreExceptions() // tables appear once the child JVM ran Flyway
                .until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%") >= 400);
        first.destroyForcibly(); // SIGKILL on Unix
        first.waitFor();
        int atKill = count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%");

        Process second = launch(group, dir.resolve("second.log"));
        try {
            await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1))
                    .until(() -> count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%") == 2 * messages);
        } finally {
            second.destroy();
            second.waitFor();
        }
        int rows = count("SELECT count(*) FROM callback_event WHERE subscriber_key LIKE ?", p + "%");
        int bounced = count("SELECT count(*) FROM message_status WHERE message_key LIKE ? AND status = 'BOUNCED'", p + "%");
        int repeatedSideEffects = count("SELECT count(*) FROM downstream_call d JOIN callback_event e USING (event_id) "
                + "WHERE e.subscriber_key LIKE ? AND d.calls > 1", p + "%");
        String report = "kill -9 IT: events=%d rowsAtKill=%d finalRows=%d bounced=%d/%d downstreamRepeated=%d%n"
                .formatted(2 * messages, atKill, rows, bounced, messages, repeatedSideEffects);
        System.out.print(report);
        Files.writeString(dir.resolve("report.txt"), report);
        assertThat(rows).isEqualTo(2 * messages);
        assertThat(bounced).isEqualTo(messages);
    }

}
