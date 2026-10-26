package dev.jagadeesh.telemetry.ingest;

import dev.jagadeesh.telemetry.config.TelemetryProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Batch consumer. Two failure paths:
 * <ul>
 *   <li>Poison records (bad JSON, failed validation) go straight to the dead-letter topic; retrying can't fix them.</li>
 *   <li>Transient failures (MongoDB or Kafka down) throw, so the container's error handler retries the whole batch
 *       with backoff and, when retries run out, dead-letters it. Retrying is safe because every write is idempotent.</li>
 * </ul>
 * Offsets are committed only after the listener returns, so nothing is lost if the pod dies mid-batch.
 */
@Component
public class TelemetryListener {

    private static final Logger log = LoggerFactory.getLogger(TelemetryListener.class);

    private final EventParser parser;
    private final TelemetryProcessor processor;
    private final KafkaTemplate<String, String> kafka;
    private final TelemetryProperties props;

    public TelemetryListener(EventParser parser, TelemetryProcessor processor, KafkaTemplate<String, String> kafka,
                             TelemetryProperties props) {
        this.parser = parser;
        this.processor = processor;
        this.kafka = kafka;
        this.props = props;
    }

    @KafkaListener(id = "telemetry-ingest", topics = "${telemetry.raw-topic}", batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        List<TelemetryEvent> valid = new ArrayList<>(records.size());
        List<ConsumerRecord<String, String>> poison = new ArrayList<>();
        List<String> reasons = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            try {
                valid.add(parser.parse(record.value()));
            } catch (InvalidEventException e) {
                poison.add(record);
                reasons.add(e.getMessage());
            }
        }

        MDC.put("batchSize", String.valueOf(records.size()));
        try {
            if (!valid.isEmpty()) {
                var result = processor.process(valid);
                log.debug("Processed batch: {}", result);
            }
            // Only after the valid part succeeded, so a retried batch doesn't dead-letter the same record twice.
            for (int i = 0; i < poison.size(); i++) {
                deadLetter(poison.get(i), reasons.get(i));
            }
        } finally {
            MDC.remove("batchSize");
        }
    }

    private void deadLetter(ConsumerRecord<String, String> record, String reason) {
        log.warn("Dead-lettering {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), reason);
        var out = new ProducerRecord<>(props.deadLetterTopic(), record.key(), record.value());
        out.headers().add("x-error", reason.getBytes(StandardCharsets.UTF_8));
        out.headers().add("x-original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("x-original-partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8));
        try {
            kafka.send(out).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Dead-letter publish failed", e);
        }
    }
}
