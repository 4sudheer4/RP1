package com.rp1.loadgen;

import com.rp1.common.EnvelopeFactory;
import com.rp1.common.EventType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LoadgenTest {

    @Test
    void lifecycleEventsAreMonotonicPerMessageAndIdsMatchIngest() {
        EventGenerator gen = new EventGenerator(7, "rt", 50, 100, 0, 0.02, 0);
        EnvelopeFactory ingest = new EnvelopeFactory(Duration.ofMinutes(5));
        Map<String, Integer> lastRank = new HashMap<>();
        Map<String, Long> lastTs = new HashMap<>();
        Set<String> ids = new HashSet<>();
        int invalid = 0;
        for (int i = 0; i < 5000; i++) {
            var g = gen.next();
            var built = ingest.build(g.event(), "b", Instant.now(), "t");
            assertThat(built instanceof EnvelopeFactory.Valid).isEqualTo(g.valid());
            if (!g.valid()) {
                invalid++;
                continue;
            }
            String id = EnvelopeFactory.eventId(g.event());
            assertThat(((EnvelopeFactory.Valid) built).envelope().eventId()).isEqualTo(id); // ledger id == ingest id
            assertThat(ids.add(id)).as("event ids unique").isTrue();
            long ts = g.event().get("timestampUTC").asLong();
            assertThat(ts).isGreaterThan(lastTs.getOrDefault(g.messageKey(), 0L));
            lastTs.put(g.messageKey(), ts);
            if (g.type().affectsStatus()) {
                assertThat(g.type().rank()).isGreaterThan(lastRank.getOrDefault(g.messageKey(), 0));
                lastRank.put(g.messageKey(), g.type().rank());
            }
        }
        assertThat(invalid).isBetween(50, 150);
        assertThat(lastRank.values()).contains(EventType.SMS_DELIVERED.rank());
    }

    @Test
    void nearestRankPercentiles() {
        var p = Stats.percentiles(List.of(5L, 1L, 4L, 2L, 3L, 6L, 7L, 8L, 9L, 10L));
        assertThat(p).containsEntry("p50", 5L).containsEntry("p90", 9L).containsEntry("max", 10L);
    }

    @Test
    void prometheusDeltaIgnoresBucketsAndUnchanged() {
        String before = "rp1_ingest_responses_total{status=\"200\"} 10.0\nrp1_ingest_x_bucket{le=\"1\"} 3\n";
        String after = "rp1_ingest_responses_total{status=\"200\"} 25.0\nrp1_ingest_x_bucket{le=\"1\"} 9\nrp1_ingest_same 1\n";
        assertThat(Stats.promDelta(before + "rp1_ingest_same 1\n", after, "rp1_ingest_"))
                .containsOnlyKeys("rp1_ingest_responses_total{status=\"200\"}")
                .containsEntry("rp1_ingest_responses_total{status=\"200\"}", 15.0);
    }
}
