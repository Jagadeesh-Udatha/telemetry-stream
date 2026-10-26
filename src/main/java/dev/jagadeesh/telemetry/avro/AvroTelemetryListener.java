package dev.jagadeesh.telemetry.avro;

import dev.jagadeesh.telemetry.ingest.EventParser;
import dev.jagadeesh.telemetry.ingest.InvalidEventException;
import dev.jagadeesh.telemetry.ingest.TelemetryEvent;
import dev.jagadeesh.telemetry.ingest.TelemetryProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Batch consumer for telemetry.raw.avro. Decoded readings go through the same validation and the same
 * TelemetryProcessor as JSON events, so storage, dedupe and alerts don't care about the wire format.
 * Poison records (undecodable bytes, or decoded but invalid) go to telemetry.raw.avro.DLT without retries.
 */
@Component
@ConditionalOnProperty(name = "telemetry.avro.enabled", havingValue = "true")
public class AvroTelemetryListener {

    private static final Logger log = LoggerFactory.getLogger(AvroTelemetryListener.class);
    private static final LogAccessor LOG_ACCESSOR = new LogAccessor(AvroTelemetryListener.class);

    private final EventParser parser;
    private final TelemetryProcessor processor;
    private final KafkaTemplate<String, Object> deadLetters;
    private final AvroProperties props;

    public AvroTelemetryListener(EventParser parser, TelemetryProcessor processor,
                                 AvroDeadLetters deadLetters, AvroProperties props) {
        this.parser = parser;
        this.processor = processor;
        this.deadLetters = deadLetters.template();
        this.props = props;
    }

    @KafkaListener(id = "telemetry-ingest-avro", topics = "${telemetry.avro.topic}",
            containerFactory = AvroIngestConfig.FACTORY)
    public void onBatch(List<ConsumerRecord<String, GpsReading>> records) {
        List<TelemetryEvent> valid = new ArrayList<>(records.size());
        List<ProducerRecord<String, Object>> poison = new ArrayList<>();
        for (ConsumerRecord<String, GpsReading> record : records) {
            if (record.value() == null) {
                DeserializationException ex = SerializationUtils.getExceptionFromHeader(record,
                        SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, LOG_ACCESSOR);
                byte[] raw = ex != null && ex.getData() != null ? ex.getData() : new byte[0];
                String reason = "Undecodable Avro: " + (ex != null && ex.getCause() != null
                        ? ex.getCause().getMessage() : "null value");
                poison.add(deadLetter(record, raw, reason));
                continue;
            }
            try {
                valid.add(parser.validate(toEvent(record.value())));
            } catch (InvalidEventException e) {
                poison.add(deadLetter(record, record.value(), e.getMessage()));
            }
        }
        if (!valid.isEmpty()) {
            var result = processor.process(valid);
            log.debug("Processed Avro batch: {}", result);
        }
        // After the valid part succeeded, so a retried batch doesn't dead-letter the same record twice.
        for (ProducerRecord<String, Object> out : poison) {
            log.warn("Dead-lettering Avro record: {}", new String(out.headers().lastHeader("x-error").value(),
                    StandardCharsets.UTF_8));
            try {
                deadLetters.send(out).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("Dead-letter publish failed", e);
            }
        }
    }

    /** Avro → the internal event. fuelPct is null for v1 writers: schema resolution filled in the field's default. */
    static TelemetryEvent toEvent(GpsReading r) {
        return new TelemetryEvent(r.getEventId(), r.getVehicleId(), r.getTs(), r.getLat(), r.getLon(),
                r.getSpeedKph(), r.getHeading(), r.getFuelPct());
    }

    private ProducerRecord<String, Object> deadLetter(ConsumerRecord<String, GpsReading> record, Object value,
                                                      String reason) {
        var out = new ProducerRecord<String, Object>(props.deadLetterTopic(), record.key(), value);
        out.headers().add("x-error", reason.getBytes(StandardCharsets.UTF_8));
        out.headers().add("x-original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("x-original-partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8));
        return out;
    }
}
