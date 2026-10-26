package dev.jagadeesh.telemetry.simulator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jagadeesh.telemetry.config.TelemetryProperties;
import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Demo load: N vehicles driving around Bengaluru, one reading each per second. Mixes in the cases the pipeline must
 * survive: duplicate deliveries, occasional overspeeding, and malformed records.
 * Enable with --spring.profiles.active=simulator.
 */
@Component
@Profile("simulator")
public class FleetSimulator {

    private static final double BLR_LAT = 12.9716;
    private static final double BLR_LON = 77.5946;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final TelemetryProperties props;
    private final double[][] positions;

    public FleetSimulator(KafkaTemplate<String, String> kafka, ObjectMapper json, TelemetryProperties props,
                          @Value("${simulator.vehicles:200}") int vehicles) {
        this.kafka = kafka;
        this.json = json;
        this.props = props;
        this.positions = new double[vehicles][];
        var rnd = ThreadLocalRandom.current();
        for (int i = 0; i < vehicles; i++) {
            positions[i] = new double[]{BLR_LAT + rnd.nextDouble(-0.1, 0.1), BLR_LON + rnd.nextDouble(-0.1, 0.1)};
        }
    }

    @Scheduled(fixedRate = 1000)
    public void tick() throws JsonProcessingException {
        var rnd = ThreadLocalRandom.current();
        Instant now = Instant.now();
        for (int i = 0; i < positions.length; i++) {
            positions[i][0] += rnd.nextDouble(-0.0005, 0.0005);
            positions[i][1] += rnd.nextDouble(-0.0005, 0.0005);
            double speed = rnd.nextDouble() < 0.01 ? rnd.nextDouble(85, 120) : rnd.nextDouble(0, 60);
            String vehicleId = "KA-%04d".formatted(i);
            var event = new TelemetryEvent(UUID.randomUUID().toString(), vehicleId, now,
                    positions[i][0], positions[i][1], speed, rnd.nextDouble(0, 360));
            String payload = json.writeValueAsString(event);
            kafka.send(props.rawTopic(), vehicleId, payload);
            if (rnd.nextDouble() < 0.02) {
                kafka.send(props.rawTopic(), vehicleId, payload);  // duplicate delivery
            }
        }
        if (rnd.nextDouble() < 0.1) {
            kafka.send(props.rawTopic(), "broken", "{not json");
        }
    }
}
