package dev.jagadeesh.telemetry.ingest;

/** A record that can never be processed (bad JSON or failed validation). Retrying it is pointless. */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
