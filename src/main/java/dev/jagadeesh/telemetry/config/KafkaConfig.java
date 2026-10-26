package dev.jagadeesh.telemetry.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.time.Clock;

@Configuration
public class KafkaConfig {

    /**
     * Picked up by Spring Boot's listener container factory. Retries a failed batch with exponential backoff
     * (200 ms, 400 ms, 800 ms …), then publishes each record of the batch to the dead-letter topic and moves on,
     * so one bad batch can't block the partition forever.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, TelemetryProperties props) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(props.deadLetterTopic(), -1));
        var backOff = new ExponentialBackOffWithMaxRetries(props.maxRetries());
        backOff.setInitialInterval(200);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5_000);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    /** Partitions cap consumer parallelism: 12 partitions allow up to 12 consumer pods/threads. */
    @Bean
    NewTopic rawTopic(TelemetryProperties props) {
        return TopicBuilder.name(props.rawTopic()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic alertsTopic(TelemetryProperties props) {
        return TopicBuilder.name(props.alertsTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic(TelemetryProperties props) {
        return TopicBuilder.name(props.deadLetterTopic()).partitions(1).replicas(1).build();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
