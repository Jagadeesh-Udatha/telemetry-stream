package dev.jagadeesh.telemetry.store;

import dev.jagadeesh.telemetry.config.TelemetryProperties;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;

/** Creates the indexes the read paths rely on. Idempotent: MongoDB ignores an index that already exists. */
@Component
@ConditionalOnProperty(name = "telemetry.create-indexes", havingValue = "true", matchIfMissing = true)
public class IndexInitializer implements ApplicationRunner {

    private final MongoTemplate mongo;
    private final TelemetryProperties props;

    public IndexInitializer(MongoTemplate mongo, TelemetryProperties props) {
        this.mongo = mongo;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Track queries: one vehicle, a time range, newest first.
        mongo.indexOps(TelemetryPoint.class).ensureIndex(
                new Index().on("vehicleId", Sort.Direction.ASC).on("ts", Sort.Direction.DESC).named("vehicle_ts"));
        // Raw points expire automatically after the retention window.
        mongo.indexOps(TelemetryPoint.class).ensureIndex(
                new Index().on("receivedAt", Sort.Direction.ASC).expire(props.retention()).named("ttl_received"));
        // "Which vehicles are near this point?"
        mongo.indexOps(VehicleState.class).ensureIndex(
                new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("location_2dsphere"));
        mongo.indexOps(Alert.class).ensureIndex(
                new Index().on("vehicleId", Sort.Direction.ASC).on("ts", Sort.Direction.DESC).named("vehicle_ts"));
        mongo.indexOps(Alert.class).ensureIndex(new Index().on("ts", Sort.Direction.DESC).named("ts"));
    }
}
