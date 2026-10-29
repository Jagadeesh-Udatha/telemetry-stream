package dev.jagadeesh.telemetry.ingest;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * One GPS reading from a vehicle, published as JSON on the raw topic.
 * Boxed numbers so a missing field fails validation. fuelPct is optional: older devices don't send it.
 */
public record TelemetryEvent(
        @NotBlank @Size(max = 64) String eventId,
        @NotBlank @Size(max = 32) String vehicleId,
        @NotNull Instant ts,
        @NotNull @DecimalMin("-90") @DecimalMax("90") Double lat,
        @NotNull @DecimalMin("-180") @DecimalMax("180") Double lon,
        @NotNull @DecimalMin("0") @DecimalMax("300") Double speedKph,
        @NotNull @DecimalMin("0") @DecimalMax("360") Double heading,
        @DecimalMin("0") @DecimalMax("100") Double fuelPct) {

    public TelemetryEvent(String eventId, String vehicleId, Instant ts, Double lat, Double lon, Double speedKph,
                          Double heading) {
        this(eventId, vehicleId, ts, lat, lon, speedKph, heading, null);
    }
}
