package dev.jagadeesh.telemetry.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

@Component
public class EventParser {

    private final ObjectMapper json;
    private final Validator validator;

    public EventParser(ObjectMapper json, Validator validator) {
        this.json = json;
        this.validator = validator;
    }

    public TelemetryEvent parse(String payload) {
        TelemetryEvent event;
        try {
            event = json.readValue(payload, TelemetryEvent.class);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Malformed JSON: " + e.getOriginalMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("Malformed JSON: " + e.getMessage(), e);
        }
        if (event == null) {
            throw new InvalidEventException("Empty payload", null);
        }
        return validate(event);
    }

    /** Same rules for every wire format, so a JSON and an Avro event are accepted or rejected alike. */
    public TelemetryEvent validate(TelemetryEvent event) {
        Set<ConstraintViolation<TelemetryEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new InvalidEventException("Invalid event: " + detail, null);
        }
        return event;
    }
}
