package com.rp1.consumer;

import com.rp1.common.Topics;
import com.rp1.consumer.config.ConsumerProperties;
import com.rp1.consumer.engine.CallbackListener;
import com.rp1.consumer.engine.RateLimiter;
import com.rp1.consumer.engine.ThrottleGuard;
import com.rp1.consumer.infra.LagAgeMonitor;
import com.rp1.sink.CallbackSink;
import com.rp1.sink.SinkConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;

@SpringBootApplication
@EnableConfigurationProperties(ConsumerProperties.class)
@Import(SinkConfiguration.class)
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @Bean
    ThrottleGuard throttleGuard(KafkaListenerEndpointRegistry registry, TaskScheduler scheduler, ConsumerProperties props, MeterRegistry meters) {
        return new ThrottleGuard(registry, scheduler, props.throttle(), CallbackListener.MAIN_ID, meters);
    }

    @Bean
    CallbackListener callbackListener(CallbackSink sink, ThrottleGuard throttle, ConsumerProperties props, MeterRegistry meters) {
        RateLimiter limiter = props.maxRps() > 0 ? new RateLimiter(props.maxRps(), Duration.ofSeconds(1)) : null;
        return new CallbackListener(sink, throttle, limiter, meters);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "rp1.consumer.lag-monitor.enabled", havingValue = "true", matchIfMissing = true)
    LagAgeMonitor lagAgeMonitor(@Value("${spring.kafka.bootstrap-servers}") String bootstrap,
                                @Value("${spring.kafka.consumer.group-id}") String group,
                                TaskScheduler scheduler, MeterRegistry meters) {
        LagAgeMonitor m = new LagAgeMonitor(bootstrap, group, Topics.CALLBACK_EVENTS, meters);
        scheduler.scheduleWithFixedDelay(m::sample, Duration.ofSeconds(10));
        return m;
    }
}
