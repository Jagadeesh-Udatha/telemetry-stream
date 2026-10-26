package dev.jagadeesh.telemetry.store;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Component
public class TelemetryQueries {

    private final MongoTemplate mongo;

    public TelemetryQueries(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    public Optional<VehicleState> state(String vehicleId) {
        return Optional.ofNullable(mongo.findById(vehicleId, VehicleState.class));
    }

    /** Uses the (vehicleId, ts) index. */
    public List<TelemetryPoint> track(String vehicleId, Instant from, Instant to, int limit) {
        Query q = query(where("vehicleId").is(vehicleId).and("ts").gte(from).lt(to))
                .with(Sort.by("ts")).limit(limit);
        return mongo.find(q, TelemetryPoint.class);
    }

    /** Uses the 2dsphere index; distance in metres, nearest first. */
    public List<VehicleState> near(double lat, double lon, double radiusKm, int limit) {
        Query q = query(where("location").nearSphere(new GeoJsonPoint(lon, lat)).maxDistance(radiusKm * 1000))
                .limit(limit);
        return mongo.find(q, VehicleState.class);
    }

    public List<Alert> alerts(String vehicleId, int limit) {
        Query q = vehicleId == null ? new Query() : query(where("vehicleId").is(vehicleId));
        return mongo.find(q.with(Sort.by(Sort.Direction.DESC, "ts")).limit(limit), Alert.class);
    }
}
