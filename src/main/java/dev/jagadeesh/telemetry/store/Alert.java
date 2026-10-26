package dev.jagadeesh.telemetry.store;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** Raised by a rule. The id is derived from the event id, so re-processing an event never duplicates its alert. */
@Document("alerts")
public record Alert(@Id String id, String vehicleId, String type, Instant ts, GeoJsonPoint location,
                    double speedKph, String detail) {

    public static String idFor(String eventId, String type) {
        return eventId + ":" + type;
    }
}
