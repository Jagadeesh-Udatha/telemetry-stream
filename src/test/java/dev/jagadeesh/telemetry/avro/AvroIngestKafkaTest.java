package dev.jagadeesh.telemetry.avro;

import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import dev.jagadeesh.telemetry.store.TelemetryWriter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/**
 * Real Kafka (embedded) through the real Avro listener container, with Confluent's in-memory Schema Registry
 * ({@code mock://}): the same serializers and wire format as production, minus the registry server.
 * A v1 device and a v2 device write to the same topic; both are stored, and bad records are dead-lettered.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "telemetry.avro.enabled=true",
        "telemetry.avro.schema-registry-url=mock://avro-ingest-test"})
@EmbeddedKafka(partitions = 1, topics = {"telemetry.raw", "telemetry.raw.DLT", "telemetry.alerts",
        "telemetry.raw.avro", "telemetry.raw.avro.DLT"})
@ActiveProfiles("test")
class AvroIngestKafkaTest {

    static final String REGISTRY = "mock://avro-ingest-test";
    static final Instant TS = Instant.parse("2026-10-26T04:00:00Z");

    @Autowired EmbeddedKafkaBroker broker;
    @MockitoBean TelemetryWriter writer;

    @Test
    void v1AndV2DevicesShareTheTopicAndPoisonGoesToTheDlt() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        Map<String, TelemetryEvent> stored = new ConcurrentHashMap<>();
        when(writer.insertPoints(anyList(), any())).thenAnswer(inv -> {
            List<TelemetryEvent> batch = inv.getArgument(0);
            batch.forEach(e -> stored.put(e.eventId(), e));
            return batch.size();
        });
        when(writer.advanceState(any(), any())).thenReturn(true);
        String key = run + "-v";

        try (var avro = avroProducer(); var raw = rawProducer()) {
            // An old device: writes with the v1 schema (registers v1 under telemetry.raw.avro-value).
            avro.send(new ProducerRecord<>("telemetry.raw.avro", key, v1Record(run + "-old")));
            // A new device: v2 with fuel level.
            avro.send(new ProducerRecord<>("telemetry.raw.avro", key, GpsReading.newBuilder()
                    .setEventId(run + "-new").setVehicleId(key).setTs(TS).setLat(12.97).setLon(77.59)
                    .setSpeedKph(40).setHeading(90).setFuelPct(63.5).build()));
            // Valid Avro, invalid data: latitude out of range.
            avro.send(new ProducerRecord<>("telemetry.raw.avro", key, GpsReading.newBuilder()
                    .setEventId(run + "-badlat").setVehicleId(key).setTs(TS).setLat(120).setLon(77.59)
                    .setSpeedKph(40).setHeading(90).build()));
            // Not Avro at all (no magic byte): a JSON producer pointed at the wrong topic.
            raw.send(new ProducerRecord<>("telemetry.raw.avro", key, "{\"eventId\":\"x\"}".getBytes(StandardCharsets.UTF_8)));
            avro.flush();
            raw.flush();
        }

        await().atMost(Duration.ofSeconds(20)).until(() -> stored.containsKey(run + "-old") && stored.containsKey(run + "-new"));
        assertThat(stored.get(run + "-old").fuelPct()).isNull();
        assertThat(stored.get(run + "-new").fuelPct()).isEqualTo(63.5);
        assertThat(stored.get(run + "-old").ts()).isEqualTo(TS);
        assertThat(stored).doesNotContainKey(run + "-badlat");

        List<ConsumerRecord<String, byte[]>> dlt = readAll("telemetry.raw.avro.DLT", key, 2);
        assertThat(dlt).hasSize(2);
        assertThat(dlt).extracting(r -> header(r, "x-error")).anySatisfy(e -> assertThat(e).contains("lat must be less than or equal to 90"));
        assertThat(dlt).extracting(r -> header(r, "x-error")).anySatisfy(e -> assertThat(e).startsWith("Undecodable Avro"));
        // The undecodable record is kept byte-for-byte so it can be inspected and replayed.
        assertThat(dlt).anySatisfy(r -> assertThat(new String(r.value(), StandardCharsets.UTF_8)).isEqualTo("{\"eventId\":\"x\"}"));
    }

    private GenericRecord v1Record(String eventId) {
        Schema v1 = SchemaEvolutionTest.V1;
        GenericRecord r = new GenericData.Record(v1);
        r.put("eventId", eventId);
        r.put("vehicleId", eventId.substring(0, eventId.lastIndexOf('-')) + "-v");
        r.put("ts", TS.toEpochMilli());
        r.put("lat", 12.97);
        r.put("lon", 77.59);
        r.put("speedKph", 30.0);
        r.put("heading", 45.0);
        return r;
    }

    private KafkaProducer<String, Object> avroProducer() {
        Map<String, Object> props = KafkaTestUtils.producerProps(broker);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY);
        var serializer = new KafkaAvroSerializer();
        serializer.configure(props, false);
        return new KafkaProducer<>(props, new StringSerializer(), serializer);
    }

    private KafkaProducer<String, byte[]> rawProducer() {
        return new KafkaProducer<>(KafkaTestUtils.producerProps(broker), new StringSerializer(), new ByteArraySerializer());
    }

    private List<ConsumerRecord<String, byte[]>> readAll(String topic, String key, int expected) {
        List<ConsumerRecord<String, byte[]>> matches = new ArrayList<>();
        try (Consumer<String, byte[]> consumer = new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps("reader-" + UUID.randomUUID(), "false", broker),
                new StringDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, topic);
            long deadline = System.currentTimeMillis() + 20_000;
            while (matches.size() < expected && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(r.key())) {
                        matches.add(r);
                    }
                }
            }
        }
        return matches;
    }

    private static String header(ConsumerRecord<String, byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
