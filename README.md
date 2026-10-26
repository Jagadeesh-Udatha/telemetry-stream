# Telemetry Stream

High-throughput vehicle telemetry ingestion: a **Kafka batch consumer** writes GPS events to **MongoDB** with **idempotent bulk upserts**, keeps a live position per vehicle, raises overspeed alerts to a second topic, and sends poison records to a **dead-letter topic**. Ships with **Kubernetes** manifests (probes, HPA, PDB), Prometheus metrics and JSON logs.

Java 21 · Spring Boot 3.4 · Spring Kafka · Spring Data MongoDB · Micrometer/Prometheus · JUnit 5 · Mockito · Embedded Kafka · Testcontainers · Docker · Kubernetes · GitHub Actions

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

## API

| Endpoint | Purpose | Index used |
|---|---|---|
| `GET /api/vehicles/{id}` | Latest position | `_id` |
| `GET /api/vehicles/{id}/track?from=&to=&limit=` | Route replay (max 7 days) | `{vehicleId: 1, ts: -1}` |
| `GET /api/vehicles/near?lat=&lon=&radiusKm=` | Vehicles near a point | `2dsphere` on `location` |
| `GET /api/alerts?vehicleId=&limit=` | Recent alerts | `{vehicleId: 1, ts: -1}` |

## Metrics

`telemetry_events_received_total`, `telemetry_events_stored_total`, `telemetry_events_duplicate_total`, `telemetry_state_stale_total`, `telemetry_alerts_raised_total`, `telemetry_batch_duration_seconds` (p50/p95/p99), plus Spring Kafka listener and consumer-lag metrics.

## Tests

```bash
mvn verify
```

- `TelemetryProcessorTest` (unit): in-batch dedupe, one state write per vehicle, stale detection, alert ids.
- `TelemetryListenerKafkaTest` (embedded Kafka, real listener container): poison records to DLT with reasons, alerts published, transient failure retried, permanent failure dead-lettered.
- `TelemetryWriterMongoTest` (real MongoDB via Testcontainers, or `MONGO_TEST_URI`): idempotent inserts, state never moves back in time, idempotent alerts, geo query.

See [INTERVIEW_NOTES.md](INTERVIEW_NOTES.md).
