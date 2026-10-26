package dev.jagadeesh.telemetry.store;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** Latest known position per vehicle. Only moves forward in time, even when events arrive out of order. */
@Document("vehicle_state")
public record VehicleState(@Id String vehicleId, String lastEventId, Instant lastTs, GeoJsonPoint location,
                           double speedKph, double heading, Instant updatedAt) {
}
