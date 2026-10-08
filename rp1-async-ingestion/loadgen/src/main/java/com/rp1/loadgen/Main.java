package com.rp1.loadgen;

import com.rp1.common.Json;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

/**
 * <pre>
 * loadgen run    --url URL --rate BATCHES_PER_S --duration S --out DIR [--batch-size 50] [--timeout-ms 2000]
 *                [--retries 3] [--retry-delays-ms 1000,2000,4000] [--subscribers 5000] [--active-messages 2000]
 *                [--hot-fraction 0] [--invalid-fraction 0] [--poison 0] [--metrics-url URL]
 *                [--suspend-after 0 --suspend-ms 600000] [--seed 42]      key: RP1_SIGNATURE_KEY or --key
 * loadgen verify --run DIR --jdbc URL [--user rp1 --password rp1] [--bootstrap HOST:PORT] [--max-wait-s 300] [--stable-s 10]
 * loadgen dlq-replay --bootstrap HOST:PORT [--from callback-events-dlt] [--to callback-events]
 *                [--group rp1-dlq-replay] [--max N] [--dry-run]
 * </pre>
 */
public final class Main {
    private Main() {}

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("usage: loadgen run|verify|dlq-replay ... (see Main javadoc)");
            System.exit(2);
        }
        Args a = new Args(argv, 1);
        Map<String, Object> result;
        Path out;
        switch (argv[0]) {
            case "run" -> {
                out = Path.of(a.required("out"));
                String key = a.str("key", System.getenv("RP1_SIGNATURE_KEY"));
                if (key == null) throw new IllegalArgumentException("signing key: set RP1_SIGNATURE_KEY or --key");
                long[] delays = Arrays.stream(a.str("retry-delays-ms", "1000,2000,4000").split(",")).mapToLong(Long::parseLong).toArray();
                result = new Sender(new Sender.Config(a.required("url"), a.dbl("rate", 10), a.integer("duration", 30),
                        a.integer("batch-size", 50), Duration.ofMillis(a.lng("timeout-ms", 2000)), a.integer("retries", 3),
                        delays, a.integer("subscribers", 5000), a.integer("active-messages", 2000), a.dbl("hot-fraction", 0),
                        a.dbl("invalid-fraction", 0), a.integer("poison", 0), key, a.str("key-encoding", "UTF8"), out,
                        a.str("metrics-url", null), a.integer("suspend-after", 0), a.lng("suspend-ms", 600_000),
                        a.lng("seed", 42))).run();
                write(out.resolve("summary.json"), result);
            }
            case "verify" -> {
                out = Path.of(a.required("run"));
                result = new Verifier(new Verifier.Config(out, a.required("jdbc"), a.str("user", "rp1"),
                        a.str("password", "rp1"), a.str("bootstrap", null), Duration.ofSeconds(a.lng("max-wait-s", 300)),
                        Duration.ofSeconds(a.lng("stable-s", 10)))).verify();
                write(out.resolve("verify.json"), result);
            }
            case "dlq-replay" -> {
                result = new DlqReplay(new DlqReplay.Config(a.required("bootstrap"), a.str("from", "callback-events-dlt"),
                        a.str("to", "callback-events"), a.str("group", "rp1-dlq-replay"), a.lng("max", Long.MAX_VALUE),
                        a.flag("dry-run"))).run();
            }
            default -> throw new IllegalArgumentException("unknown command " + argv[0]);
        }
        System.out.println(Json.MAPPER.writer().with(SerializationFeature.INDENT_OUTPUT).writeValueAsString(result));
        System.exit(0); // HttpClient/Kafka threads must not keep the JVM alive
    }

    private static void write(Path p, Map<String, Object> m) throws Exception {
        Files.writeString(p, Json.MAPPER.writer().with(SerializationFeature.INDENT_OUTPUT).writeValueAsString(m));
    }
}
