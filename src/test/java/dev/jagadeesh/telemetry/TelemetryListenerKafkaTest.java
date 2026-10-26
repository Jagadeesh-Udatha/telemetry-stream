package dev.jagadeesh.telemetry;

import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import dev.jagadeesh.telemetry.store.TelemetryWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/** Real Kafka (embedded broker) through the real listener container; MongoDB writes are mocked. */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {"telemetry.raw", "telemetry.raw.DLT", "telemetry.alerts"})
@ActiveProfiles("test")
class TelemetryListenerKafkaTest {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EmbeddedKafkaBroker broker;
    @MockitoBean TelemetryWriter writer;

    @Test
    void validEventsAreStoredPoisonGoesToDltAndOverspeedIsPublished() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        List<String> stored = new CopyOnWriteArrayList<>();
        when(writer.insertPoints(anyList(), any())).thenAnswer(inv -> {
            List<TelemetryEvent> batch = inv.getArgument(0);
            batch.forEach(e -> stored.add(e.eventId()));
            return batch.size();
        });
        when(writer.advanceState(any(), any())).thenReturn(true);

        send(run + "-v", json(run + "-ok1", run + "-v", 30));
        send(run + "-v", json(run + "-fast", run + "-v", 101.5));
        send(run + "-v", "{not json");
        send(run + "-v", json(run + "-badlat", run + "-v", 30).replace("\"lat\":12.97", "\"lat\":120"));

        await().atMost(Duration.ofSeconds(15)).until(() -> stored.containsAll(List.of(run + "-ok1", run + "-fast")));
        assertThat(stored).noneMatch(id -> id.endsWith("-badlat"));

        List<ConsumerRecord<String, String>> dlt = readAll("telemetry.raw.DLT", run + "-v", 2);
        assertThat(dlt).hasSize(2);
        assertThat(header(dlt.get(0), "x-error")).startsWith("Malformed JSON");
        assertThat(header(dlt.get(1), "x-error")).contains("lat must be less than or equal to 90");

        List<ConsumerRecord<String, String>> alerts = readAll("telemetry.alerts", run + "-v", 1);
        assertThat(alerts).singleElement().satisfies(a -> assertThat(a.value()).contains(run + "-fast:OVERSPEED"));
    }

    @Test
    void transientFailureIsRetriedThenSucceeds() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        AtomicInteger calls = new AtomicInteger();
        List<String> stored = new CopyOnWriteArrayList<>();
        when(writer.insertPoints(anyList(), any())).thenAnswer(inv -> {
            if (calls.incrementAndGet() == 1) {
                throw new org.springframework.dao.DataAccessResourceFailureException("MongoDB unavailable");
            }
            List<TelemetryEvent> batch = inv.getArgument(0);
            batch.forEach(e -> stored.add(e.eventId()));
            return batch.size();
        });
        when(writer.advanceState(any(), any())).thenReturn(true);

        send(run + "-v", json(run + "-retry", run + "-v", 20));

        await().atMost(Duration.ofSeconds(15)).until(() -> stored.contains(run + "-retry"));
        assertThat(calls.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void permanentFailureEndsInDeadLetterTopic() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        when(writer.insertPoints(anyList(), any()))
                .thenThrow(new org.springframework.dao.DataAccessResourceFailureException("MongoDB down"));

        send(run + "-dead", json(run + "-lost", run + "-dead", 20));

        List<ConsumerRecord<String, String>> dlt = readAll("telemetry.raw.DLT", run + "-dead", 1);
        assertThat(dlt).singleElement().satisfies(r -> assertThat(r.value()).contains(run + "-lost"));
    }

    private void send(String key, String value) {
        kafka.send("telemetry.raw", key, value);
    }

    private static String json(String eventId, String vehicleId, double speed) {
        return """
                {"eventId":"%s","vehicleId":"%s","ts":"2026-10-26T04:00:00Z","lat":12.97,"lon":77.59,"speedKph":%s,"heading":90}"""
                .formatted(eventId, vehicleId, speed);
    }

    private List<ConsumerRecord<String, String>> readAll(String topic, String key, int expected) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps("reader-" + UUID.randomUUID(), "false", broker),
                new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, topic);
            long deadline = System.currentTimeMillis() + 20_000;
            while (matches.size() < expected && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(r.key())) {
                        matches.add(r);
                    }
                }
            }
        }
        return matches;
    }

    private static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
