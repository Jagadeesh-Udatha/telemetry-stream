package dev.jagadeesh.telemetry.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @param rawTopic        incoming GPS events
 * @param alertsTopic     alerts produced by the rules
 * @param overspeedKph    speed above which an OVERSPEED alert is raised
 * @param retention       how long raw points are kept (MongoDB TTL index)
 * @param createIndexes   create MongoDB indexes on startup
 * @param maxRetries      retries of a failed batch before its records go to the dead-letter topic
 */
@ConfigurationProperties("telemetry")
public record TelemetryProperties(String rawTopic, String alertsTopic, double overspeedKph, Duration retention,
                                  boolean createIndexes, int maxRetries) {

    public String deadLetterTopic() {
        return rawTopic + ".DLT";
    }
}
