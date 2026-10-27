package dev.jagadeesh.telemetry;

import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import dev.jagadeesh.telemetry.store.TelemetryWriter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/**
 * Tracing end to end on a real (embedded) broker: the batch span is exported, and the alert it publishes carries
 * a W3C traceparent header pointing at that same trace, so a downstream consumer continues it.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {"telemetry.raw", "telemetry.raw.DLT", "telemetry.alerts"})
@AutoConfigureObservability
@ActiveProfiles("test")
class TracePropagationKafkaTest {

    @TestConfiguration
    static class Spans {
        // Boot adds every SpanExporter bean to its span processor, next to OTLP when that is configured.
        @Bean
        InMemorySpanExporter inMemorySpans() {
            return InMemorySpanExporter.create();
        }
    }

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired InMemorySpanExporter spans;
    @MockitoBean TelemetryWriter writer;

    @Test
    void alertCarriesTheTraceOfTheBatchThatRaisedIt() {
        String run = UUID.randomUUID().toString().substring(0, 8);
        when(writer.insertPoints(anyList(), any())).thenAnswer(inv -> ((List<TelemetryEvent>) inv.getArgument(0)).size());
        when(writer.advanceState(any(), any())).thenReturn(true);
        when(writer.insertAlerts(anyList())).thenAnswer(inv -> ((List<?>) inv.getArgument(0)).size());

        kafka.send("telemetry.raw", run + "-v", """
                {"eventId":"%s-fast","vehicleId":"%s-v","ts":"2026-10-26T04:00:00Z","lat":12.97,"lon":77.59,"speedKph":120,"heading":90}"""
                .formatted(run, run));

        ConsumerRecord<String, String> alert = readAlert(run + "-v");
        String traceparent = new String(alert.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8);
        String alertTraceId = traceparent.split("-")[1];

        SpanData batch = await().atMost(Duration.ofSeconds(15))
                .until(() -> processSpanIn(alertTraceId), Optional::isPresent).orElseThrow();
        assertThat(batch.getAttributes().get(AttributeKey.stringKey("batch.size"))).isEqualTo("1");

        // The producer span for the alert send is a child of the batch span, in the same trace.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(spans.getFinishedSpanItems())
                .anySatisfy(s -> {
                    assertThat(s.getTraceId()).isEqualTo(alertTraceId);
                    assertThat(s.getName()).contains("telemetry.alerts");
                    assertThat(s.getParentSpanId()).isEqualTo(batch.getSpanId());
                }));
    }

    private Optional<SpanData> processSpanIn(String traceId) {
        return spans.getFinishedSpanItems().stream()
                .filter(s -> s.getTraceId().equals(traceId) && s.getName().equals("telemetry process batch"))
                .findFirst();
    }

    private ConsumerRecord<String, String> readAlert(String key) {
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps("trace-reader-" + UUID.randomUUID(), "false", broker),
                new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, "telemetry.alerts");
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(r.key())) {
                        return r;
                    }
                }
            }
        }
        throw new AssertionError("No alert published for " + key);
    }
}
