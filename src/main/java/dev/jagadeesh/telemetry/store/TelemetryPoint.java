package dev.jagadeesh.telemetry.store;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** A stored GPS point. The event id is the document id, which makes re-delivered events harmless. */
@Document("telemetry_points")
public record TelemetryPoint(@Id String eventId, String vehicleId, Instant ts, GeoJsonPoint location,
                             double speedKph, double heading, Instant receivedAt) {
}
