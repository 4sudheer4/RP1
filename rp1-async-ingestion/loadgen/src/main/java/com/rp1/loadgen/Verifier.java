package com.rp1.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.rp1.common.Json;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.postgresql.PGConnection;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ledger vs system of record. Definitions (reported, never hard-coded):
 * <ul>
 *   <li>eventCapture = distinct ledger valid event ids present in callback_event / valid events generated</li>
 *   <li>lost = valid generated - persisted - (accounted for in DLT); duplicates = ids with >1 row (0 by PK)</li>
 *   <li>statusMismatches = messages whose status events all persisted but whose message_status differs from
 *       the lifecycle's final state (catches out-of-order regressions)</li>
 * </ul>
 * Batch-level capture (first attempt / eventual 2xx) comes from the sender's summary.json.
 */
final class Verifier {

    record Config(Path runDir, String jdbcUrl, String user, String password, String bootstrap,
                  Duration maxWait, Duration stableFor) {}

    private final Config cfg;

    Verifier(Config cfg) {
        this.cfg = cfg;
    }

    Map<String, Object> verify() throws Exception {
        Path events = cfg.runDir().resolve("events.csv");
        String runId = runId(events);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("runId", runId);
        try (Connection c = DriverManager.getConnection(cfg.jdbcUrl(), cfg.user(), cfg.password());
             Statement st = c.createStatement()) {
            st.execute("""
                    CREATE TEMP TABLE ledger (event_id varchar(160) PRIMARY KEY, batch_id text, valid boolean,
                      message_key text, event_type text, rank int, status text, poison boolean)""");
            try (Reader rd = Files.newBufferedReader(events)) {
                c.unwrap(PGConnection.class).getCopyAPI()
                        .copyIn("COPY ledger FROM STDIN WITH (FORMAT csv, HEADER true)", rd);
            }
            long persisted = waitForDrain(st);
            long generated = one(st, "SELECT count(*) FROM ledger");
            long valid = one(st, "SELECT count(*) FROM ledger WHERE valid");
            long poison = one(st, "SELECT count(*) FROM ledger WHERE valid AND poison");
            long dupIds = one(st, """
                    SELECT count(*) FROM (SELECT e.event_id FROM callback_event e JOIN ledger l USING (event_id)
                    GROUP BY e.event_id HAVING count(*) > 1) d""");
            Set<String> missing = new HashSet<>();
            try (ResultSet rs = st.executeQuery("""
                    SELECT l.event_id FROM ledger l WHERE l.valid
                    AND NOT EXISTS (SELECT 1 FROM callback_event e WHERE e.event_id = l.event_id)""")) {
                while (rs.next()) missing.add(rs.getString(1));
            }
            Map<String, Long> status = statusCheck(st);

            long invalidPersisted = one(st, "SELECT count(*) FROM invalid_event WHERE batch_id LIKE '" + runId + "-%'");
            Long repeatedDownstream = tableExists(st, "downstream_call")
                    ? one(st, "SELECT count(*) FROM downstream_call d JOIN ledger USING (event_id) WHERE d.calls > 1") : null;

            Set<String> dltIds = new HashSet<>();
            long invalidTopic = -1;
            if (cfg.bootstrap() != null) {
                dltIds = eventIdsInTopic("callback-events-dlt", runId);
                invalidTopic = countInTopic("invalid-events", runId);
            }
            long missingInDlt = missing.stream().filter(dltIds::contains).count();

            r.put("eventsGenerated", generated);
            r.put("validEvents", valid);
            r.put("invalidEventsGenerated", generated - valid);
            r.put("poisonEvents", poison);
            r.put("persistedDistinct", persisted);
            r.put("duplicateRows", dupIds);
            r.put("missing", missing.size());
            r.put("missingButInDlt", missingInDlt);
            r.put("lostUnaccounted", missing.size() - missingInDlt);
            r.put("eventCapture", ratio(persisted, valid));
            r.put("eventCaptureExcludingPoison", ratio(persisted, valid - poison));
            r.put("dltRecordsForRun", dltIds.size());
            r.put("invalidRoutedToTopic", invalidTopic);
            r.put("invalidRecordedSyncMode", invalidPersisted);
            r.putAll(status);
            r.put("downstreamRepeatedCalls", repeatedDownstream);
        }
        Path summary = cfg.runDir().resolve("summary.json");
        if (Files.exists(summary)) {
            JsonNode s = Json.MAPPER.readTree(summary.toFile());
            r.put("batchCaptureFirstAttempt", s.path("batchCaptureFirstAttempt").asDouble());
            r.put("batchCaptureEventual", s.path("batchCaptureEventual").asDouble());
        }
        return r;
    }

    /** Polls until the persisted count stops changing (consumers drained) or maxWait passes. */
    private long waitForDrain(Statement st) throws Exception {
        String q = "SELECT count(*) FROM ledger l WHERE EXISTS (SELECT 1 FROM callback_event e WHERE e.event_id = l.event_id)";
        long deadline = System.currentTimeMillis() + cfg.maxWait().toMillis();
        long last = -1, stableSince = System.currentTimeMillis();
        while (true) {
            long now = one(st, q);
            if (now != last) {
                last = now;
                stableSince = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - stableSince >= cfg.stableFor().toMillis() || System.currentTimeMillis() > deadline) return now;
            Thread.sleep(1000);
        }
    }

    private Map<String, Long> statusCheck(Statement st) throws Exception {
        String base = """
                WITH exp AS (SELECT message_key, count(*) n FROM ledger WHERE valid AND rank > 0 AND NOT poison GROUP BY message_key),
                     got AS (SELECT l.message_key, count(*) n FROM ledger l JOIN callback_event e USING (event_id)
                             WHERE l.rank > 0 GROUP BY l.message_key),
                     fin AS (SELECT DISTINCT ON (message_key) message_key, status FROM ledger
                             WHERE valid AND rank > 0 ORDER BY message_key, rank DESC),
                     complete AS (SELECT exp.message_key FROM exp JOIN got USING (message_key) WHERE exp.n = got.n)
                """;
        long checked = one(st, base + "SELECT count(*) FROM complete");
        long mismatched = one(st, base + """
                SELECT count(*) FROM complete JOIN fin USING (message_key)
                LEFT JOIN message_status ms USING (message_key) WHERE ms.status IS DISTINCT FROM fin.status""");
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("messagesFullyPersistedChecked", checked);
        m.put("statusMismatches", mismatched);
        return m;
    }

    private Set<String> eventIdsInTopic(String topic, String runId) {
        Set<String> ids = new HashSet<>();
        scan(topic, r -> {
            JsonNode n = parse(r.value());
            if (n != null && n.path("batchId").asText("").startsWith(runId + "-")) ids.add(n.path("eventId").asText());
        });
        return ids;
    }

    private long countInTopic(String topic, String runId) {
        long[] n = {0};
        scan(topic, r -> {
            JsonNode j = parse(r.value());
            if (j != null && j.path("batchId").asText("").startsWith(runId + "-")) n[0]++;
        });
        return n[0];
    }

    private void scan(String topic, java.util.function.Consumer<ConsumerRecord<byte[], byte[]>> f) {
        try (var c = new KafkaConsumer<byte[], byte[]>(Map.of("bootstrap.servers", cfg.bootstrap(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            while (tps.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                c.poll(Duration.ofMillis(500)).forEach(f);
            }
        }
    }

    private static JsonNode parse(byte[] v) {
        try {
            return v == null ? null : Json.MAPPER.readTree(v);
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean tableExists(Statement st, String table) throws Exception {
        try (ResultSet rs = st.executeQuery("SELECT to_regclass('" + table + "') IS NOT NULL")) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    private static long one(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String runId(Path events) throws Exception {
        try (var lines = Files.lines(events)) {
            String row = lines.skip(1).findFirst().orElseThrow(() -> new IllegalStateException("empty ledger"));
            String batchId = row.split(",")[1];
            return batchId.substring(0, batchId.lastIndexOf('-'));
        }
    }

    private static double ratio(long a, long b) {
        return b == 0 ? 0 : Math.round(1_000_000.0 * a / b) / 1_000_000.0;
    }
}
