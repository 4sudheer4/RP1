package com.rp1.consumer.config;

import com.rp1.common.Headers;
import com.rp1.common.Topics;
import com.rp1.common.errors.NonRetryableException;
import com.rp1.common.errors.RetryableException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Non-blocking retries (Spring Kafka retry topics). A failing record is re-published to
 * callback-events-retry-{0,1,2} with jittered exponential backoff and the main partition keeps moving, so one bad
 * record never stalls the subscribers behind it. Exhausted or non-retryable -> callback-events-dlt.
 *
 * <p>Trade-off: a retried event leaves its partition, so it can be applied after later events for the same
 * subscriber. That is safe only because the sink's status guard is order-independent (see CallbackSink).
 *
 * <p>DLT records carry Spring's kafka_original-topic/-partition/-offset/-timestamp, kafka_exception-fqcn/-message,
 * kafka_dlt-original-consumer-group headers, plus ours: attempt count, first-failure time, and error class.
 */
@Configuration(proxyBeanMethods = false)
public class RetryTopicConfig extends RetryTopicConfigurationSupport {

    @Bean
    RetryTopicConfiguration callbackRetryTopics(KafkaTemplate<Object, Object> template, ConsumerProperties props) {
        ConsumerProperties.Retry r = props.retry();
        return RetryTopicConfigurationBuilder.newInstance()
                .maxAttempts(r.attempts())
                .exponentialBackoff(r.initialBackoff().toMillis(), r.multiplier(), r.maxBackoff().toMillis(), true) // jitter
                .retryTopicSuffix(Topics.RETRY_SUFFIX)
                .dltSuffix(Topics.DLT_SUFFIX)
                .suffixTopicsWithIndexValues()
                .doNotAutoCreateRetryTopics()        // topics are code: infra/kafka/create-topics.sh (RF3, minISR2)
                .includeTopic(Topics.CALLBACK_EVENTS)
                .notRetryOn(List.of(NonRetryableException.class, DeserializationException.class))
                .traversingCauses()
                .concurrency(2)
                .create(template);
    }

    @Override
    protected void configureCustomizers(CustomizersConfigurer customizers) {
        customizers.customizeDeadLetterPublishingRecoverer(dlpr -> dlpr.setHeadersFunction((record, ex) -> {
            RecordHeaders h = new RecordHeaders();
            var prevAttempts = record.headers().lastHeader(Headers.ATTEMPTS);
            int attempts = prevAttempts == null ? 1 : ByteBuffer.wrap(prevAttempts.value()).getInt() + 1;
            h.add(new RecordHeader(Headers.ATTEMPTS, ByteBuffer.allocate(4).putInt(attempts).array()));
            if (record.headers().lastHeader(Headers.FIRST_FAILURE_TS) == null) {
                h.add(new RecordHeader(Headers.FIRST_FAILURE_TS,
                        Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));
            }
            h.add(new RecordHeader(Headers.ERROR_CLASS, classify(ex).getBytes(StandardCharsets.UTF_8)));
            return h;
        }));
    }

    @Override
    protected void manageNonBlockingFatalExceptions(List<Class<? extends Throwable>> fatal) {
        fatal.add(NonRetryableException.class);
    }

    /** Retry topics schedule delayed resumption of paused partitions; also used by ThrottleGuard. */
    @Bean
    TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(2);
        s.setThreadNamePrefix("rp1-sched-");
        return s;
    }

    static String classify(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof NonRetryableException || t instanceof DeserializationException) return "non_retryable";
            if (t instanceof RetryableException) return "retryable";
        }
        return "unclassified";
    }
}
