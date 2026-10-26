package dev.jagadeesh.telemetry.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jagadeesh.telemetry.config.TelemetryProperties;
import dev.jagadeesh.telemetry.store.Alert;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Publishes alerts keyed by vehicle id. At-least-once: consumers dedupe on the alert-id header. */
@Component
public class AlertPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final TelemetryProperties props;

    public AlertPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json, TelemetryProperties props) {
        this.kafka = kafka;
        this.json = json;
        this.props = props;
    }

    public void publish(List<Alert> alerts) {
        CompletableFuture<?>[] sends = alerts.stream().map(a -> {
            var record = new ProducerRecord<>(props.alertsTopic(), a.vehicleId(), toJson(a));
            record.headers().add("alert-id", a.id().getBytes(StandardCharsets.UTF_8));
            return kafka.send(record);
        }).toArray(CompletableFuture[]::new);
        // Wait for all acks before the batch's offsets are committed; a failure fails (and retries) the batch.
        try {
            CompletableFuture.allOf(sends).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Alert publish failed", e);
        }
    }

    private String toJson(Alert a) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("alertId", a.id());
        body.put("vehicleId", a.vehicleId());
        body.put("type", a.type());
        body.put("ts", a.ts().toString());
        body.put("lat", a.location().getY());
        body.put("lon", a.location().getX());
        body.put("speedKph", a.speedKph());
        body.put("detail", a.detail());
        try {
            return json.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
