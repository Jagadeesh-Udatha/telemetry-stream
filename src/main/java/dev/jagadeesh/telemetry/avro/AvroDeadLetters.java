package dev.jagadeesh.telemetry.avro;

import org.springframework.kafka.core.KafkaTemplate;

/**
 * Holder for the dead-letter template. It isn't a KafkaTemplate bean itself, because a second KafkaTemplate bean
 * would switch off Spring Boot's auto-configured String template that the JSON path uses.
 */
public record AvroDeadLetters(KafkaTemplate<String, Object> template) {
}
