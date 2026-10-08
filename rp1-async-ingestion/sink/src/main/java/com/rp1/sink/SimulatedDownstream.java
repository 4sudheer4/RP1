package com.rp1.sink;

import com.rp1.common.CallbackEnvelope;
import com.rp1.common.errors.NonRetryableException;
import com.rp1.common.errors.RetryableException;
import com.rp1.common.errors.ThrottledException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Fixed-capacity, fixed-service-time downstream with optional fault injection. See {@link SinkProperties}. */
public class SimulatedDownstream implements Downstream {
    private final SinkProperties props;
    private final Semaphore permits;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate outsideTx;
    private final Counter ok, throttled, transientFail, permanentFail;

    public SimulatedDownstream(SinkProperties props, JdbcTemplate jdbc, TransactionTemplate tx, MeterRegistry registry) {
        this.props = props;
        this.permits = new Semaphore(props.capacity(), true);
        this.jdbc = jdbc;
        this.outsideTx = new TransactionTemplate(tx.getTransactionManager());
        this.outsideTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.ok = Counter.builder("rp1.downstream.calls").tag("result", "ok").register(registry);
        this.throttled = Counter.builder("rp1.downstream.calls").tag("result", "throttled").register(registry);
        this.transientFail = Counter.builder("rp1.downstream.calls").tag("result", "transient").register(registry);
        this.permanentFail = Counter.builder("rp1.downstream.calls").tag("result", "permanent").register(registry);
        Gauge.builder("rp1.downstream.inflight", () -> props.capacity() - permits.availablePermits()).register(registry);
        Gauge.builder("rp1.downstream.waiting", permits::getQueueLength).register(registry);
    }

    @Override
    public void call(CallbackEnvelope env) {
        if (props.faultMarkers()) {
            String sk = env.subscriberKey();
            if (sk.startsWith("poison-")) {
                permanentFail.increment();
                throw new NonRetryableException("downstream rejected event (400): poison marker");
            }
            if (sk.startsWith("flaky-")) {
                transientFail.increment();
                throw new RetryableException("downstream unavailable (503): flaky marker");
            }
        }
        if (props.transientFailureRate() > 0 && ThreadLocalRandom.current().nextDouble() < props.transientFailureRate()) {
            transientFail.increment();
            throw new RetryableException("downstream unavailable (503): injected");
        }
        boolean acquired;
        try {
            acquired = permits.tryAcquire(props.acquireTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableException("interrupted waiting for downstream", e);
        }
        if (!acquired) {
            throttled.increment();
            throw new ThrottledException("downstream saturated (429)");
        }
        try {
            sleep(serviceTimeNanos());
            if (props.recordCalls()) recordCall(env.eventId());
            ok.increment();
        } finally {
            permits.release();
        }
    }

    private long serviceTimeNanos() {
        long base = props.serviceTime().toNanos();
        long j = props.jitter().toNanos();
        return j == 0 ? base : Math.max(0, base + ThreadLocalRandom.current().nextLong(-j, j + 1));
    }

    private static void sleep(long nanos) {
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableException("interrupted during downstream call", e);
        }
    }

    /** Committed independently of the processing txn: it models a side effect that already happened. */
    private void recordCall(String eventId) {
        outsideTx.executeWithoutResult(s -> jdbc.update("""
                INSERT INTO downstream_call (event_id, calls) VALUES (?, 1)
                ON CONFLICT (event_id) DO UPDATE SET calls = downstream_call.calls + 1, last_at = now()""", eventId));
    }
}
