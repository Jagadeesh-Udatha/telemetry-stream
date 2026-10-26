package dev.jagadeesh.telemetry;

import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import dev.jagadeesh.telemetry.store.Alert;
import dev.jagadeesh.telemetry.store.IndexInitializer;
import dev.jagadeesh.telemetry.store.TelemetryQueries;
import dev.jagadeesh.telemetry.store.TelemetryWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real MongoDB. Uses MONGO_TEST_URI if set (e.g. a local mongod), otherwise a Testcontainers MongoDB 7 container.
 * Skipped when neither is available.
 */
@SpringBootTest(properties = {"spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false",
        "telemetry.create-indexes=true"})
@ActiveProfiles("test")
@EnabledIf("mongoAvailable")
class TelemetryWriterMongoTest {

    static final Instant T0 = Instant.parse("2026-10-26T04:00:00Z");
    static MongoDBContainer container;

    static boolean mongoAvailable() {
        return System.getenv("MONGO_TEST_URI") != null || DockerClientFactory.instance().isDockerAvailable();
    }

    @DynamicPropertySource
    static void mongo(DynamicPropertyRegistry registry) {
        String uri = System.getenv("MONGO_TEST_URI");
        if (uri == null) {
            container = new MongoDBContainer("mongo:7.0");
            container.start();
            uri = container.getReplicaSetUrl("telemetry_test");
        }
        String finalUri = uri;
        registry.add("spring.data.mongodb.uri", () -> finalUri);
    }

    @AfterAll
    static void stop() {
        if (container != null) {
            container.stop();
        }
    }

    @Autowired TelemetryWriter writer;
    @Autowired TelemetryQueries queries;
    @Autowired MongoTemplate mongo;
    @Autowired IndexInitializer indexes;

    @BeforeEach
    void clean() {
        mongo.getDb().drop();
        indexes.run(null);
    }

    @Test
    void insertingTheSameEventsTwiceStoresThemOnce() {
        var batch = List.of(event("e1", "KA-1", 0, 30), event("e2", "KA-1", 1, 31));

        assertThat(writer.insertPoints(batch, T0)).isEqualTo(2);
        assertThat(writer.insertPoints(batch, T0)).isZero();
        assertThat(mongo.getCollection("telemetry_points").countDocuments()).isEqualTo(2);
        assertThat(queries.track("KA-1", T0, T0.plusSeconds(60), 10))
                .extracting(p -> p.eventId()).containsExactly("e1", "e2");
    }

    @Test
    void stateNeverMovesBackInTime() {
        assertThat(writer.advanceState(event("new", "KA-7", 10, 30), T0)).isTrue();
        assertThat(writer.advanceState(event("old", "KA-7", 5, 30), T0)).isFalse();
        assertThat(writer.advanceState(event("newer", "KA-7", 20, 30), T0)).isTrue();

        assertThat(queries.state("KA-7")).get().satisfies(s -> {
            assertThat(s.lastEventId()).isEqualTo("newer");
            assertThat(s.lastTs()).isEqualTo(T0.plusSeconds(20));
        });
    }

    @Test
    void alertsAreIdempotent() {
        var alert = new Alert(Alert.idFor("e9", "OVERSPEED"), "KA-1", "OVERSPEED", T0,
                new GeoJsonPoint(77.59, 12.97), 99, "fast");

        assertThat(writer.insertAlerts(List.of(alert))).isEqualTo(1);
        assertThat(writer.insertAlerts(List.of(alert))).isZero();
        assertThat(queries.alerts("KA-1", 10)).hasSize(1);
    }

    @Test
    void nearQueryUsesGeoIndex() {
        writer.advanceState(new TelemetryEvent("a", "MG-ROAD", T0, 12.9756, 77.6066, 10.0, 0.0), T0);   // MG Road
        writer.advanceState(new TelemetryEvent("b", "WHITEFIELD", T0, 12.9698, 77.7500, 10.0, 0.0), T0); // ~15 km east

        assertThat(queries.near(12.9716, 77.5946, 3, 10))
                .extracting(s -> s.vehicleId()).containsExactly("MG-ROAD");
    }

    static TelemetryEvent event(String id, String vehicle, int seconds, double speed) {
        return new TelemetryEvent(id, vehicle, T0.plusSeconds(seconds), 12.97, 77.59, speed, 0.0);
    }
}
