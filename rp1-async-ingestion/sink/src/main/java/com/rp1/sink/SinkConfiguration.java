package com.rp1.sink;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** Imported explicitly by the apps that process events (consumer, ingest in sync mode). */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SinkProperties.class)
public class SinkConfiguration {

    @Bean
    @ConditionalOnMissingBean(Downstream.class)
    Downstream downstream(SinkProperties props, JdbcTemplate jdbc, TransactionTemplate tx, MeterRegistry registry) {
        return new SimulatedDownstream(props, jdbc, tx, registry);
    }

    @Bean
    CallbackSink callbackSink(JdbcTemplate jdbc, TransactionTemplate tx, Downstream downstream, MeterRegistry registry) {
        return new CallbackSink(jdbc, tx, downstream, registry);
    }
}
