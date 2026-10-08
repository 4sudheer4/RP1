package com.rp1.ingest.config;

import com.rp1.common.Canonical;
import com.rp1.ingest.engine.IngestPath;
import com.rp1.ingest.engine.KafkaIngestPath;
import com.rp1.ingest.engine.SignatureVerifier;
import com.rp1.ingest.engine.SyncIngestPath;
import com.rp1.sink.CallbackSink;
import com.rp1.sink.SinkConfiguration;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfig {
    private static final Logger log = LoggerFactory.getLogger(IngestConfig.class);

    @Bean
    @ConditionalOnProperty(name = "rp1.ingest.signature.enabled", havingValue = "true", matchIfMissing = true)
    SignatureVerifier signatureVerifier(IngestProperties props) {
        return new SignatureVerifier(props.signature().keys(), props.signature().keyEncoding());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "rp1.ingest.mode", havingValue = "async", matchIfMissing = true)
    static class Async {
        /**
         * Count-based breaker around produce. Opens when produce calls fail/time out or run slow, then rejects
         * instantly (503) for waitInOpenState, so a Kafka outage costs ~0ms per request instead of the full deadline.
         */
        @Bean
        CircuitBreaker produceBreaker(IngestProperties props, MeterRegistry registry) {
            IngestProperties.Breaker b = props.breaker();
            CircuitBreaker cb = CircuitBreaker.of("kafka-produce", CircuitBreakerConfig.custom()
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(b.slidingWindowSize())
                    .minimumNumberOfCalls(b.minimumNumberOfCalls())
                    .failureRateThreshold(b.failureRateThreshold())
                    .slowCallDurationThreshold(b.slowCallThreshold())
                    .slowCallRateThreshold(b.slowCallRateThreshold())
                    .waitDurationInOpenState(b.waitInOpenState())
                    .permittedNumberOfCallsInHalfOpenState(3)
                    .automaticTransitionFromOpenToHalfOpenEnabled(true)
                    .build());
            // 0 closed, 1 open, 2 half-open (alertable)
            Gauge.builder("rp1.ingest.breaker.state", cb, c -> switch (c.getState()) {
                case CLOSED -> 0;
                case OPEN, FORCED_OPEN -> 1;
                default -> 2;
            }).register(registry);
            return cb;
        }

        @Bean
        IngestPath kafkaIngestPath(KafkaTemplate<String, byte[]> kafka, CircuitBreaker breaker, IngestProperties props, MeterRegistry registry) {
            return new KafkaIngestPath(kafka, breaker, props.minProduceBudget().toNanos(), registry);
        }

        /**
         * Replaces Spring's LoggingProducerListener, which logs the record key on failure: our key is the
         * subscriberKey, often an email address (PII). Failures are already counted and logged per batch.
         */
        @Bean
        ProducerListener<Object, Object> redactingProducerListener() {
            return new ProducerListener<>() {
                @Override
                public void onError(ProducerRecord<Object, Object> record, RecordMetadata metadata, Exception exception) {
                    log.debug("send failed topic={} key={} error={}", record.topic(), Canonical.redact(String.valueOf(record.key())), exception.toString());
                }
            };
        }

        @Bean
        ProducerReadiness producerReadiness(KafkaTemplate<String, byte[]> kafka) {
            return new ProducerReadiness(kafka);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "rp1.ingest.mode", havingValue = "sync")
    @Import(SinkConfiguration.class)
    static class Sync {
        @Bean
        IngestPath syncIngestPath(CallbackSink sink, IngestProperties props, MeterRegistry registry) {
            return new SyncIngestPath(sink, props.sync().clientTimeout(), registry);
        }
    }
}
