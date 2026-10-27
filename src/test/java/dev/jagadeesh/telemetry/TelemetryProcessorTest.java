package dev.jagadeesh.telemetry;

import dev.jagadeesh.telemetry.config.TelemetryProperties;
import dev.jagadeesh.telemetry.ingest.AlertPublisher;
import dev.jagadeesh.telemetry.ingest.AlertRules;
import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import dev.jagadeesh.telemetry.ingest.TelemetryProcessor;
import dev.jagadeesh.telemetry.store.Alert;
import dev.jagadeesh.telemetry.store.TelemetryWriter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TelemetryProcessorTest {

    static final Instant T0 = Instant.parse("2026-10-26T04:00:00Z");

    TelemetryWriter writer = mock(TelemetryWriter.class);
    AlertPublisher publisher = mock(AlertPublisher.class);
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    TelemetryProcessor processor;

    @BeforeEach
    void setUp() {
        var props = new TelemetryProperties("raw", "alerts", 80, Duration.ofDays(30), false, 3);
        processor = new TelemetryProcessor(writer, new AlertRules(props), publisher, meters,
                Clock.fixed(T0, ZoneOffset.UTC), ObservationRegistry.NOOP);
        when(writer.advanceState(any(), any())).thenReturn(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void duplicatesInsideOneBatchAreWrittenOnce() {
        when(writer.insertPoints(anyList(), any())).thenAnswer(inv -> ((List<?>) inv.getArgument(0)).size());
        var e1 = event("e1", "KA-1", 0, 40);
        var e2 = event("e2", "KA-1", 1, 42);

        var result = processor.process(List.of(e1, e2, e1));

        ArgumentCaptor<List<TelemetryEvent>> written = ArgumentCaptor.forClass(List.class);
        verify(writer).insertPoints(written.capture(), any());
        assertThat(written.getValue()).extracting(TelemetryEvent::eventId).containsExactly("e1", "e2");
        assertThat(result.duplicates()).isEqualTo(1);
        assertThat(meters.counter("telemetry.events.received").count()).isEqualTo(3);
    }

    @Test
    void redeliveredBatchCountsEverythingAsDuplicate() {
        when(writer.insertPoints(anyList(), any())).thenReturn(0);  // MongoDB already has them

        var result = processor.process(List.of(event("e1", "KA-1", 0, 40), event("e2", "KA-2", 0, 40)));

        assertThat(result.stored()).isZero();
        assertThat(result.duplicates()).isEqualTo(2);
    }

    @Test
    void vehicleStateIsUpdatedOncePerVehicleWithItsNewestEvent() {
        var older = event("e1", "KA-1", 5, 40);
        var newest = event("e2", "KA-1", 9, 41);
        var middle = event("e3", "KA-1", 7, 42);
        var other = event("e4", "KA-2", 1, 30);

        processor.process(List.of(older, newest, middle, other));

        verify(writer).advanceState(newest, T0);
        verify(writer).advanceState(other, T0);
        verify(writer, times(2)).advanceState(any(), any());
    }

    @Test
    void staleStateIsCounted() {
        when(writer.advanceState(any(), any())).thenReturn(false);

        var result = processor.process(List.of(event("e1", "KA-1", 0, 40)));

        assertThat(result.staleStates()).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void overspeedRaisesAlertWithStableId() {
        when(writer.insertAlerts(anyList())).thenReturn(1);

        processor.process(List.of(event("e1", "KA-1", 0, 95.5), event("e2", "KA-2", 0, 79.9)));

        ArgumentCaptor<List<Alert>> alerts = ArgumentCaptor.forClass(List.class);
        verify(publisher).publish(alerts.capture());
        assertThat(alerts.getValue()).singleElement().satisfies(a -> {
            assertThat(a.id()).isEqualTo("e1:OVERSPEED");
            assertThat(a.vehicleId()).isEqualTo("KA-1");
            assertThat(a.detail()).contains("95.5");
        });
    }

    @Test
    void noAlertsMeansNothingPublished() {
        processor.process(List.of(event("e1", "KA-1", 0, 30)));
        verify(publisher, never()).publish(anyList());
    }

    static TelemetryEvent event(String id, String vehicle, int secondsAfterT0, double speed) {
        return new TelemetryEvent(id, vehicle, T0.plusSeconds(secondsAfterT0), 12.97, 77.59, speed, 90.0);
    }
}
