package com.rp1.sink;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.EventType;
import com.rp1.common.errors.NonRetryableException;
import com.rp1.common.errors.RetryableException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

/**
 * Idempotent processing of one event, in one Postgres transaction:
 * <ol>
 *   <li>insert into {@code callback_event} ON CONFLICT DO NOTHING; 0 rows = duplicate, stop (dedup hit)</li>
 *   <li>downstream side effect</li>
 *   <li>guarded upsert of {@code message_status} (only moves forward in (rank, ordering_ts, event_id))</li>
 * </ol>
 * Commit happens before the caller acknowledges the Kafka offset, so a crash at any point means redelivery, and
 * redelivery is absorbed by step 1. Two consumers racing on the same event serialize on the primary key: the
 * second INSERT waits for the first txn and then sees the conflict.
 */
public class CallbackSink {

    public enum Outcome { APPLIED, DUPLICATE, STALE_STATUS_IGNORED }

    /** Where the event came from, recorded for traceability. */
    public record Source(String kind, Integer partition, Long offset) {
        public static final Source SYNC = new Source("sync", null, null);
        public static Source kafka(int partition, long offset) { return new Source("kafka", partition, offset); }
    }

    private static final String INSERT_EVENT = """
            INSERT INTO callback_event (event_id, batch_id, event_type, subscriber_key, message_key, upstream_ts,
                ordering_ts, clock_skewed, received_at, trace_id, source, kafka_partition, kafka_offset, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (event_id) DO NOTHING""";

    // Row comparison makes the guard a strict total order; WHERE false -> 0 rows updated -> stale event.
    private static final String UPSERT_STATUS = """
            INSERT INTO message_status (message_key, subscriber_key, channel, status, status_rank, ordering_ts, last_event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (message_key) DO UPDATE SET
                status = EXCLUDED.status, status_rank = EXCLUDED.status_rank, ordering_ts = EXCLUDED.ordering_ts,
                last_event_id = EXCLUDED.last_event_id, transitions = message_status.transitions + 1, updated_at = now()
            WHERE (message_status.status_rank, message_status.ordering_ts, message_status.last_event_id)
                < (EXCLUDED.status_rank, EXCLUDED.ordering_ts, EXCLUDED.last_event_id)""";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Downstream downstream;
    private final Map<Outcome, Counter> outcomes;
    private final Timer timer;

    public CallbackSink(JdbcTemplate jdbc, TransactionTemplate tx, Downstream downstream, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.downstream = downstream;
        this.outcomes = Map.of(
                Outcome.APPLIED, Counter.builder("rp1.sink.events").tag("outcome", "applied").register(registry),
                Outcome.DUPLICATE, Counter.builder("rp1.sink.events").tag("outcome", "duplicate").register(registry),
                Outcome.STALE_STATUS_IGNORED, Counter.builder("rp1.sink.events").tag("outcome", "stale_status").register(registry));
        this.timer = Timer.builder("rp1.sink.process").publishPercentileHistogram().register(registry);
    }

    public Outcome process(CallbackEnvelope e, Source source) {
        Outcome outcome = timer.record(() -> {
            try {
                return tx.execute(status -> processInTx(e, source));
            } catch (DataIntegrityViolationException ex) {
                // Not the dedup key (that is ON CONFLICT): a real constraint/data problem; retrying cannot fix it.
                throw new NonRetryableException("constraint violation: " + ex.getMostSpecificCause().getClass().getSimpleName(), ex);
            } catch (DataAccessException ex) {
                // Connection loss, timeouts, deadlocks, serialization failures: transient by default.
                throw new RetryableException("database error: " + ex.getClass().getSimpleName(), ex);
            }
        });
        outcomes.get(outcome).increment();
        return outcome;
    }

    private Outcome processInTx(CallbackEnvelope e, Source source) {
        int inserted = jdbc.update(INSERT_EVENT, e.eventId(), e.batchId(), e.eventType(), e.subscriberKey(),
                e.messageKey(), ts(e.upstreamTs()), ts(e.orderingTs()), e.clockSkewed(), ts(e.receivedAt()),
                e.traceId(), source.kind(), source.partition(), source.offset(), e.event().toString());
        if (inserted == 0) return Outcome.DUPLICATE;

        downstream.call(e);

        EventType type = EventType.fromWire(e.eventType())
                .orElseThrow(() -> new NonRetryableException("unknown event type reached sink"));
        if (!type.affectsStatus()) return Outcome.APPLIED;
        int moved = jdbc.update(UPSERT_STATUS, e.messageKey(), e.subscriberKey(), type.channel().name(),
                type.status(), type.rank(), ts(e.orderingTs()), e.eventId());
        return moved == 1 ? Outcome.APPLIED : Outcome.STALE_STATUS_IGNORED;
    }

    /** Sync-baseline mode only; async mode publishes to the invalid-events topic instead. */
    public void recordInvalid(String batchId, String reason, Instant receivedAt, String eventJson) {
        jdbc.update("INSERT INTO invalid_event (batch_id, reason, received_at, payload) VALUES (?, ?, ?, ?::jsonb)",
                batchId, reason, ts(receivedAt), eventJson);
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }
}
