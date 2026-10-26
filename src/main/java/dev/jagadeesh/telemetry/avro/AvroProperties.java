package dev.jagadeesh.telemetry.avro;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param enabled           consume Avro readings from {@code topic} (off by default; JSON on telemetry.raw keeps working)
 * @param topic             Avro topic; its subject in Schema Registry is {@code <topic>-value}
 * @param schemaRegistryUrl Schema Registry base URL ({@code mock://name} in tests)
 */
@ConfigurationProperties("telemetry.avro")
public record AvroProperties(boolean enabled, String topic, String schemaRegistryUrl) {

    public String deadLetterTopic() {
        return topic + ".DLT";
    }
}
