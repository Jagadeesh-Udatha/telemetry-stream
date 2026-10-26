package dev.jagadeesh.telemetry.api;

import dev.jagadeesh.telemetry.store.Alert;
import dev.jagadeesh.telemetry.store.TelemetryPoint;
import dev.jagadeesh.telemetry.store.TelemetryQueries;
import dev.jagadeesh.telemetry.store.VehicleState;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api")
@Validated
public class VehicleController {

    private final TelemetryQueries queries;

    public VehicleController(TelemetryQueries queries) {
        this.queries = queries;
    }

    @GetMapping("/vehicles/{vehicleId}")
    public VehicleState state(@PathVariable String vehicleId) {
        return queries.state(vehicleId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown vehicle " + vehicleId));
    }

    @GetMapping("/vehicles/{vehicleId}/track")
    public List<TelemetryPoint> track(@PathVariable String vehicleId,
                                      @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                                      @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
                                      @RequestParam(defaultValue = "1000") @Min(1) @Max(5000) int limit) {
        if (!from.isBefore(to) || Duration.between(from, to).toDays() > 7) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "from must be before to, at most 7 days apart");
        }
        return queries.track(vehicleId, from, to, limit);
    }

    @GetMapping("/vehicles/near")
    public List<VehicleState> near(@RequestParam @DecimalMin("-90") @DecimalMax("90") double lat,
                                   @RequestParam @DecimalMin("-180") @DecimalMax("180") double lon,
                                   @RequestParam(defaultValue = "2") @DecimalMin("0.1") @DecimalMax("50") double radiusKm,
                                   @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        return queries.near(lat, lon, radiusKm, limit);
    }

    @GetMapping("/alerts")
    public List<Alert> alerts(@RequestParam(required = false) String vehicleId,
                              @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        return queries.alerts(vehicleId, limit);
    }
}
