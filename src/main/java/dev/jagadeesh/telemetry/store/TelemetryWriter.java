package dev.jagadeesh.telemetry.store;

import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * All MongoDB writes. Every write is idempotent so a Kafka batch can be re-delivered or retried safely:
 * points and alerts are upserted by a natural id with $setOnInsert, and vehicle state only moves forward in time.
 */
@Component
public class TelemetryWriter {

    private final MongoTemplate mongo;

    public TelemetryWriter(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /** One unordered bulk round trip for the whole batch. Returns how many points were new. */
    public int insertPoints(List<TelemetryEvent> events, Instant receivedAt) {
        if (events.isEmpty()) {
            return 0;
        }
        BulkOperations bulk = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, TelemetryPoint.class);
        for (TelemetryEvent e : events) {
            Update insertOnly = new Update()
                    .setOnInsert("vehicleId", e.vehicleId())
                    .setOnInsert("ts", e.ts())
                    .setOnInsert("location", point(e))
                    .setOnInsert("speedKph", e.speedKph())
                    .setOnInsert("heading", e.heading())
                    .setOnInsert("receivedAt", receivedAt);
            if (e.fuelPct() != null) {
                insertOnly.setOnInsert("fuelPct", e.fuelPct());
            }
            bulk.upsert(query(where("_id").is(e.eventId())), insertOnly);
        }
        return bulk.execute().getUpserts().size();
    }

    /**
     * Moves the vehicle's latest position forward if this event is newer than what is stored.
     * The filter only matches an older state; if a newer one exists, the upsert tries to insert a second document
     * with the same _id and MongoDB rejects it, which we treat as "stale event, ignore".
     *
     * @return false when a newer position was already stored
     */
    public boolean advanceState(TelemetryEvent e, Instant now) {
        Update update = new Update()
                .set("lastEventId", e.eventId())
                .set("lastTs", e.ts())
                .set("location", point(e))
                .set("speedKph", e.speedKph())
                .set("heading", e.heading())
                .set("updatedAt", now);
        try {
            mongo.upsert(query(where("_id").is(e.vehicleId()).and("lastTs").lt(e.ts())), update, VehicleState.class);
            return true;
        } catch (DuplicateKeyException stale) {
            return false;
        }
    }

    /** Returns how many alerts were new. */
    public int insertAlerts(List<Alert> alerts) {
        if (alerts.isEmpty()) {
            return 0;
        }
        BulkOperations bulk = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, Alert.class);
        for (Alert a : alerts) {
            Update insertOnly = new Update()
                    .setOnInsert("vehicleId", a.vehicleId())
                    .setOnInsert("type", a.type())
                    .setOnInsert("ts", a.ts())
                    .setOnInsert("location", a.location())
                    .setOnInsert("speedKph", a.speedKph())
                    .setOnInsert("detail", a.detail());
            bulk.upsert(query(where("_id").is(a.id())), insertOnly);
        }
        return bulk.execute().getUpserts().size();
    }

    static GeoJsonPoint point(TelemetryEvent e) {
        return new GeoJsonPoint(e.lon(), e.lat());  // GeoJSON order is [longitude, latitude]
    }
}
