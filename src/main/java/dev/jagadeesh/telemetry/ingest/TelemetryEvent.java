package dev.jagadeesh.telemetry.ingest;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/** One GPS reading from a vehicle, as published on the raw topic. Boxed numbers so a missing field fails validation. */
public record TelemetryEvent(
        @NotBlank @Size(max = 64) String eventId,
        @NotBlank @Size(max = 32) String vehicleId,
        @NotNull Instant ts,
        @NotNull @DecimalMin("-90") @DecimalMax("90") Double lat,
        @NotNull @DecimalMin("-180") @DecimalMax("180") Double lon,
        @NotNull @DecimalMin("0") @DecimalMax("300") Double speedKph,
        @NotNull @DecimalMin("0") @DecimalMax("360") Double heading) {
}
