# Telemetry Stream

High-throughput vehicle telemetry ingestion: a **Kafka batch consumer** writes GPS events to **MongoDB** with **idempotent bulk upserts**, keeps a live position per vehicle, raises overspeed alerts to a second topic, and sends poison records to a **dead-letter topic**. Ships with **Kubernetes** manifests (probes, HPA, PDB) and full observability: **OpenTelemetry** traces in **Tempo**, Prometheus metrics on a provisioned **Grafana** dashboard, and ECS JSON logs in **Elasticsearch/Kibana** (via Fluent Bit) carrying the trace id.

Devices can also send **Avro** to `telemetry.raw.avro`, with schemas in **Confluent Schema Registry** under **BACKWARD** compatibility, so old (v1) and new (v2) devices share one topic.

Java 21 · Spring Boot 3.4 · Spring Kafka · Apache Avro · Confluent Schema Registry · Spring Data MongoDB · Micrometer · OpenTelemetry · Prometheus · Grafana · Tempo · ELK · JUnit 5 · Mockito · Embedded Kafka · Testcontainers · Docker · Kubernetes · GitHub Actions

## Pipeline

```
vehicles ──▶ telemetry.raw (12 partitions, key = vehicleId)
                │
                ▼  @KafkaListener(batch = true), up to 500 records per poll
        parse + validate ── invalid ──▶ telemetry.raw.DLT  (x-error header)
                │
                ▼  TelemetryProcessor
        1. dedupe inside the batch
        2. bulk upsert points  ($setOnInsert, _id = eventId)      ──▶ telemetry_points (TTL 30d)
        3. one state write per vehicle, only if newer             ──▶ vehicle_state (2dsphere)
        4. rules → alerts (_id = eventId:TYPE)                    ──▶ alerts + telemetry.alerts topic
                │
        offsets committed after the batch succeeds
        failure → retry batch 200/400/800 ms → still failing → DLT
```

## Avro + Schema Registry

```
v1 device ─┐  [magic 0][schema id 1][Avro bytes]
           ├──▶ telemetry.raw.avro ──▶ KafkaAvroDeserializer ──▶ GpsReading (reader = v2) ──▶ same validation + processor
v2 device ─┘  [magic 0][schema id 2][Avro bytes]        │
                                                        └─ fetches writer schema by id from Schema Registry (cached)
```

- **The contract is a file:** [`src/main/avro/gps_reading.avsc`](src/main/avro/gps_reading.avsc). `avro-maven-plugin` generates the `GpsReading` class from it at build time.
- **v1 → v2:** v2 added `fuelPct` as `["null","double"]` with `default: null`. A v2 consumer reading a v1 record gets `fuelPct = null` (schema resolution fills the default); a v1 consumer reading v2 data skips the field. So the change is both backward and forward compatible.
- **The registry enforces it:** the subject `telemetry.raw.avro-value` is set to `BACKWARD`. Registering a schema that adds a field without a default, or changes a field's type, is rejected with HTTP 409 before any producer can use it.
- **Bad bytes don't stop the partition:** the Avro deserializer is wrapped in `ErrorHandlingDeserializer`. A record without the magic byte, or with an unknown schema id, reaches the listener as a null value and goes to `telemetry.raw.avro.DLT` with its original bytes and the reason in `x-error`. Decoded records that fail validation are dead-lettered as Avro.
- **One pipeline:** Avro readings are mapped to the same `TelemetryEvent`, validated by the same rules and written by the same idempotent `TelemetryProcessor` as JSON events. `fuelPct` is stored on the point when present.
- Off by default (`AVRO_ENABLED=false`); `docker compose up` turns it on.

Send Avro by hand once the stack is up:

```bash
echo '{"eventId":"demo-1","vehicleId":"KA-0001","ts":1793000000000,"lat":12.97,"lon":77.59,"speedKph":42,"heading":90,"fuelPct":{"double":63.5}}' |
docker compose exec -T schema-registry kafka-avro-console-producer \
  --bootstrap-server kafka:9092 --topic telemetry.raw.avro \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema="$(tr -d '\n' < src/main/avro/gps_reading.avsc)"
curl localhost:8081/subjects/telemetry.raw.avro-value/versions
curl localhost:8081/config   # {"compatibilityLevel":"BACKWARD"}
```

## Reliability decisions

| Situation | What happens |
|---|---|
| Kafka redelivers a batch (rebalance, crash before commit) | Points are upserted by event id with `$setOnInsert`, alerts by `eventId:TYPE`: rewriting changes nothing. |
| Events for one vehicle arrive out of order | The state upsert only matches a document with an older `lastTs`; an older event hits a duplicate-key error and is ignored. |
| Malformed JSON or out-of-range coordinates | Sent straight to `telemetry.raw.DLT` with the reason in a header. No retries, the partition keeps moving. |
| MongoDB is briefly down | The listener throws; `DefaultErrorHandler` retries the whole batch with exponential backoff. Safe because every write is idempotent. |
| MongoDB stays down | After the retries the batch goes to the DLT, so one outage can't block the partition forever; replay the DLT later. |
| Pod is stopped during a deploy | `terminationGracePeriodSeconds` + `preStop` let the current batch finish and commit. |

## Run it

```bash
docker compose up --build     # Kafka + MongoDB + ingestor + a 200-vehicle simulator
curl localhost:8080/api/vehicles/KA-0007
curl "localhost:8080/api/vehicles/near?lat=12.9716&lon=77.5946&radiusKm=2"
curl "localhost:8080/api/alerts?limit=20"
curl localhost:8080/actuator/prometheus | grep telemetry_
```

Kubernetes:

```bash
kubectl apply -f k8s/
```

## Observability

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up --build
```

| Signal | Path | Look at it |
|---|---|---|
| Traces | Micrometer Observation → OTel bridge → OTLP/HTTP → OpenTelemetry Collector → Tempo | Grafana → Explore → Tempo (http://localhost:3000) |
| Metrics | `/actuator/prometheus` ← Prometheus scrape | Grafana dashboard **Telemetry Stream** |
| Logs | ECS JSON on stdout → Docker fluentd driver → Fluent Bit → Elasticsearch | Kibana (http://localhost:5601), index `telemetry-logs-*` |

What is traced:

- **`telemetry process batch`**: one span per Kafka batch, attribute `batch.size`. Its timer `telemetry_process_seconds` is a histogram, so the dashboard's p95/p99 come from buckets, and Prometheus exemplars link a slow bucket straight to its trace.
- **Producer spans** for every alert and dead-letter send (`spring.kafka.template.observation-enabled`). Each record gets a W3C `traceparent` header, so whoever consumes `telemetry.alerts` continues the trace of the batch that raised the alert.
- **Logs** written during a batch carry `trace.id` / `span.id` (ECS fields), so you can go from a log line in Kibana to the trace in Tempo and back.

Decisions:

- The app only knows the collector's address (`MANAGEMENT_OTLP_TRACING_ENDPOINT`). Sampling, batching and where spans go (Tempo here; Jaeger, X-Ray or a vendor elsewhere) are collector config, changed without a redeploy.
- Sampling is 100% locally and 10% in Kubernetes (`TRACE_SAMPLING`, see `k8s/configmap.yaml`).
- Batch size stays a span attribute, never a metric tag: one tag value per batch size would explode Prometheus cardinality.
- Batch listeners consume up to 500 records from many producers at once, so the batch span starts a new trace instead of picking one producer's parent.

Dashboard panels: events/s (received, stored, duplicate), batch latency p50/p95/p99, consumer lag, alerts/min, batch errors/min, JVM heap.

## API

| Endpoint | Purpose | Index used |
|---|---|---|
| `GET /api/vehicles/{id}` | Latest position | `_id` |
| `GET /api/vehicles/{id}/track?from=&to=&limit=` | Route replay (max 7 days) | `{vehicleId: 1, ts: -1}` |
| `GET /api/vehicles/near?lat=&lon=&radiusKm=` | Vehicles near a point | `2dsphere` on `location` |
| `GET /api/alerts?vehicleId=&limit=` | Recent alerts | `{vehicleId: 1, ts: -1}` |

## Metrics

`telemetry_events_received_total`, `telemetry_events_stored_total`, `telemetry_events_duplicate_total`, `telemetry_state_stale_total`, `telemetry_alerts_raised_total`, `telemetry_batch_duration_seconds` (p50/p95/p99), `telemetry_process_seconds` (histogram, with exemplars), plus Spring Kafka listener and consumer-lag metrics.

## Tests

```bash
mvn verify
```

- `TelemetryProcessorTest` (unit): in-batch dedupe, one state write per vehicle, stale detection, alert ids.
- `TelemetryListenerKafkaTest` (embedded Kafka, real listener container): poison records to DLT with reasons, alerts published, transient failure retried, permanent failure dead-lettered.
- `TracePropagationKafkaTest` (embedded Kafka, OTel in-memory exporter): the batch span is exported with `batch.size`, the alert record's `traceparent` points at that trace, and the producer span is a child of the batch span.
- `TelemetryWriterMongoTest` (real MongoDB via Testcontainers, or `MONGO_TEST_URI`): idempotent inserts, state never moves back in time, idempotent alerts, geo query.
- `SchemaEvolutionTest` (pure Avro): a v2 reader decodes v1 bytes with `fuelPct = null`, a v1 reader skips the new field, v1/v2 are compatible both ways, and the two classic breaking changes (new field without default, changed type) are detected.
- `AvroIngestKafkaTest` (embedded Kafka + Confluent's in-memory registry, real serializers): a v1 and a v2 device share the topic and both are stored; invalid data and non-Avro bytes go to the Avro DLT, the bytes unchanged.
- `SchemaRegistryCompatibilityTest` (real Kafka + Schema Registry via Testcontainers): BACKWARD subject, v1 and v2 register as versions 1 and 2, a breaking schema is refused with 409.

See [INTERVIEW_NOTES.md](INTERVIEW_NOTES.md).
