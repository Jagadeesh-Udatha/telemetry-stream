package dev.jagadeesh.telemetry.ingest;

import dev.jagadeesh.telemetry.store.Alert;
import dev.jagadeesh.telemetry.store.TelemetryWriter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/** Turns one Kafka batch of valid events into MongoDB writes and alerts. Safe to run twice on the same batch. */
@Service
public class TelemetryProcessor {

    public record BatchResult(int received, int stored, int duplicates, int staleStates, int alerts) {
    }

    private final TelemetryWriter writer;
    private final AlertRules rules;
    private final AlertPublisher publisher;
    private final MeterRegistry meters;
    private final Clock clock;
    private final ObservationRegistry observations;
    private final Timer batchTimer;

    public TelemetryProcessor(TelemetryWriter writer, AlertRules rules, AlertPublisher publisher,
                              MeterRegistry meters, Clock clock, ObservationRegistry observations) {
        this.writer = writer;
        this.rules = rules;
        this.publisher = publisher;
        this.meters = meters;
        this.clock = clock;
        this.observations = observations;
        this.batchTimer = Timer.builder("telemetry.batch.duration").publishPercentiles(0.5, 0.95, 0.99).register(meters);
    }

    /**
     * One span per batch. Alerts are published inside it, so every alert record carries this span's traceparent
     * and a downstream consumer joins the same trace. Batch size is high-cardinality: a span attribute, not a metric tag.
     */
    public BatchResult process(List<TelemetryEvent> events) {
        return Observation.createNotStarted("telemetry.process", observations)
                .contextualName("telemetry process batch")
                .highCardinalityKeyValue("batch.size", String.valueOf(events.size()))
                .observe(() -> batchTimer.record(() -> doProcess(events)));
    }

    private BatchResult doProcess(List<TelemetryEvent> events) {
        Instant now = clock.instant();

        // The same event can appear twice in one batch (producer retry); keep the first.
        Map<String, TelemetryEvent> unique = new LinkedHashMap<>();
        events.forEach(e -> unique.putIfAbsent(e.eventId(), e));
        List<TelemetryEvent> batch = new ArrayList<>(unique.values());

        int stored = writer.insertPoints(batch, now);

        // One state write per vehicle (its newest event in the batch) instead of one per event.
        Map<String, TelemetryEvent> newestPerVehicle = batch.stream().collect(Collectors.toMap(
                TelemetryEvent::vehicleId, e -> e, BinaryOperator.maxBy(Comparator.comparing(TelemetryEvent::ts))));
        int stale = 0;
        for (TelemetryEvent e : newestPerVehicle.values()) {
            if (!writer.advanceState(e, now)) {
                stale++;
            }
        }

        List<Alert> alerts = rules.evaluate(batch);
        int newAlerts = writer.insertAlerts(alerts);
        if (!alerts.isEmpty()) {
            publisher.publish(alerts);
        }

        int duplicates = events.size() - stored;
        meters.counter("telemetry.events.received").increment(events.size());
        meters.counter("telemetry.events.stored").increment(stored);
        meters.counter("telemetry.events.duplicate").increment(duplicates);
        meters.counter("telemetry.state.stale").increment(stale);
        meters.counter("telemetry.alerts.raised").increment(newAlerts);
        return new BatchResult(events.size(), stored, duplicates, stale, newAlerts);
    }
}
