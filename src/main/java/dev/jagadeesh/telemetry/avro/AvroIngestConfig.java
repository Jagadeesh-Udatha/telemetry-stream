package dev.jagadeesh.telemetry.avro;

import dev.jagadeesh.telemetry.config.TelemetryProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Avro ingestion beside the JSON path. Values are decoded with Confluent's KafkaAvroDeserializer: each message carries
 * a 5-byte header (magic byte + schema id), the deserializer fetches that writer schema from Schema Registry once,
 * caches it, and resolves it against our reader schema (GpsReading v2). That resolution is what lets v1 devices and
 * v2 devices share one topic.
 */
@Configuration
@ConditionalOnProperty(name = "telemetry.avro.enabled", havingValue = "true")
public class AvroIngestConfig {

    public static final String FACTORY = "avroBatchFactory";

    /**
     * ErrorHandlingDeserializer wraps the Avro deserializer: bytes that aren't valid Avro (wrong magic byte, unknown
     * schema id) arrive in the listener as a null value plus an exception header instead of failing the whole poll.
     */
    @Bean
    DefaultKafkaConsumerFactory<String, GpsReading> avroConsumerFactory(KafkaProperties kafka, AvroProperties avro) {
        Map<String, Object> props = new LinkedHashMap<>(kafka.buildConsumerProperties(null));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-ingest-avro");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, avro.schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Writes dead letters: the original bytes when they couldn't be decoded, or the decoded GpsReading re-encoded as
     * Avro when it decoded fine but failed validation or processing. The DLT subject (telemetry.raw.avro.DLT-value)
     * belongs to this service, so it may register the reader schema there; the main subject is never written by us.
     */
    @Bean
    AvroDeadLetters avroDeadLetters(KafkaProperties kafka, AvroProperties avro) {
        Map<String, Object> props = new LinkedHashMap<>(kafka.buildProducerProperties(null));
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, avro.schemaRegistryUrl());
        var avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(props, false);
        Map<Class<?>, Serializer<?>> byType = new LinkedHashMap<>();
        byType.put(byte[].class, new ByteArraySerializer());
        byType.put(SpecificRecord.class, avroSerializer);
        var factory = new DefaultKafkaProducerFactory<String, Object>(props, new StringSerializer(),
                new DelegatingByTypeSerializer(byType, true));
        return new AvroDeadLetters(new KafkaTemplate<>(factory));
    }

    /** Same retry policy as the JSON path: back off on transient failures, then dead-letter and move on. */
    @Bean(FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, GpsReading> avroBatchFactory(
            DefaultKafkaConsumerFactory<String, GpsReading> consumerFactory,
            AvroDeadLetters deadLetters, AvroProperties avro,
            TelemetryProperties telemetry, KafkaProperties kafka) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, GpsReading>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(kafka.getListener().getConcurrency());
        var recoverer = new DeadLetterPublishingRecoverer(deadLetters.template(),
                (record, ex) -> new TopicPartition(avro.deadLetterTopic(), -1));
        var backOff = new ExponentialBackOffWithMaxRetries(telemetry.maxRetries());
        backOff.setInitialInterval(200);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5_000);
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, backOff));
        return factory;
    }

    @Bean
    NewTopic avroTopic(AvroProperties avro) {
        return TopicBuilder.name(avro.topic()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic avroDeadLetterTopic(AvroProperties avro) {
        return TopicBuilder.name(avro.deadLetterTopic()).partitions(1).replicas(1).build();
    }
}
