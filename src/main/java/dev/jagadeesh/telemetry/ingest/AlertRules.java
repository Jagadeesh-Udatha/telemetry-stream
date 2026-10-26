package dev.jagadeesh.telemetry.ingest;

import dev.jagadeesh.telemetry.config.TelemetryProperties;
import dev.jagadeesh.telemetry.store.Alert;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/** Stateless rules evaluated on every event. */
@Component
public class AlertRules {

    public static final String OVERSPEED = "OVERSPEED";

    private final TelemetryProperties props;

    public AlertRules(TelemetryProperties props) {
        this.props = props;
    }

    public List<Alert> evaluate(List<TelemetryEvent> events) {
        List<Alert> alerts = new ArrayList<>();
        for (TelemetryEvent e : events) {
            if (e.speedKph() > props.overspeedKph()) {
                alerts.add(new Alert(Alert.idFor(e.eventId(), OVERSPEED), e.vehicleId(), OVERSPEED, e.ts(),
                        new GeoJsonPoint(e.lon(), e.lat()), e.speedKph(),
                        "%.1f km/h over a %.0f km/h limit".formatted(e.speedKph(), props.overspeedKph())));
            }
        }
        return alerts;
    }
}
