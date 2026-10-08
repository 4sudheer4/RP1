package com.rp1.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class IngestApplication {
    private static final Logger log = LoggerFactory.getLogger(IngestApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }

    /**
     * Shutdown order on SIGTERM: (1) readiness -> REFUSING_TRAFFIC (load balancer stops routing; on k8s a preStop
     * sleep covers endpoint propagation), (2) graceful web-server shutdown stops accepting and waits for in-flight
     * requests, each of which already awaits its produce futures within the deadline, (3) producer factory is
     * destroyed, closing the producer (flushes anything still buffered). Every 200 was sent after its acks, so no
     * acked-but-unsent event can exist at exit.
     */
    @EventListener
    void onClose(ContextClosedEvent event) {
        ApplicationContext ctx = event.getApplicationContext();
        AvailabilityChangeEvent.publish(ctx, ReadinessState.REFUSING_TRAFFIC);
        log.info("shutdown: readiness set to REFUSING_TRAFFIC, draining in-flight requests");
    }
}
