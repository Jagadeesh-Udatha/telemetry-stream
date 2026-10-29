# Interview notes: Telemetry Stream

For Kafka / MongoDB / Kubernetes / high-scale ingestion interviews. Each section: a likely question, the answer, and where it lives.

## Kafka

**"How does Kafka give you ordering?"** Only within a partition. The producer key is `vehicleId`, so all events of one vehicle go to one partition and are read in order by one consumer. There's no ordering across vehicles, and we don't need it.

**"How do you scale consumers?"** One partition is read by at most one consumer in a group. `telemetry.raw` has 12 partitions, so up to 12 consumer threads across all pods do useful work; the HPA max is 12 for that reason. `spring.kafka.listener.concurrency` sets threads per pod.

**"Why a batch listener?"** One MongoDB bulk write per poll (up to 500 records) instead of 500 round trips. That's the main throughput lever. The cost: error handling is per batch, so writes must be idempotent.

**"At-most-once, at-least-once, exactly-once?"**
- Offsets are committed after the listener returns (`ack-mode: batch`) → **at-least-once**: a crash after the write but before the commit redelivers the batch.
- We get **effectively-once results** because every write is idempotent (natural ids, `$setOnInsert`, monotonic state).
- Kafka transactions (exactly-once) only cover Kafka→Kafka; they don't make a MongoDB write atomic with the offset commit.

**"What happens with a poison message?"** Without handling, the consumer fails, retries the same offset forever, and the partition stalls. Here: parse/validation errors → DLT immediately (`TelemetryListener`); infrastructure errors → `DefaultErrorHandler` retries with exponential backoff (`KafkaConfig`), then dead-letters. DLT records carry `x-error`, original partition and offset, so they can be inspected and replayed.

**"Producer settings?"** `acks=all` + `enable.idempotence=true`: no loss on leader failover, and no duplicates from producer retries. `linger.ms=5` batches sends.

**"What's consumer lag and how do you watch it?"** Latest offset minus committed offset per partition. Exposed by the Kafka client metrics in /actuator/metrics (or `kafka-consumer-groups --describe`); alert when it keeps growing. Fixes: more partitions + consumers, bigger batches, faster writes.

**"What triggers a rebalance and why do you care?"** Pods joining/leaving, missed heartbeats, `max.poll.interval.ms` exceeded by a slow batch. During a rebalance consumption pauses. The HPA has a 5-minute scale-down window so flapping doesn't cause rebalance storms.

## MongoDB

**"How do you make the write idempotent?"** `_id = eventId` + upsert with `$setOnInsert`: the first write inserts, later writes match and change nothing. `BulkWriteResult.getUpserts()` tells us how many were new, which gives the duplicate metric.

**"Out-of-order events: how does state not go backwards?"** The upsert filter is `{_id: vehicleId, lastTs: {$lt: ts}}`. If the stored state is newer, the filter doesn't match, the upsert tries to insert a second doc with the same `_id`, and MongoDB raises E11000 (duplicate key), which we treat as "stale". It's a compare-and-set in one round trip, with no read-modify-write race.

**"Which indexes and why?"**
- `{vehicleId: 1, ts: -1}`: track queries are equality on vehicle + range on time. Equality fields first, then range/sort (the **ESR rule**: Equality, Sort, Range).
- TTL on `receivedAt`: raw points expire after 30 days without a cron job.
- `2dsphere` on `vehicle_state.location`: `$nearSphere` "vehicles within 2 km". GeoJSON stores `[lon, lat]`, a classic bug if swapped.

**"How would you shard this?"** Shard `telemetry_points` on `{vehicleId: "hashed"}` or `{vehicleId: 1, ts: 1}`. Not on `ts` alone: all writes would hit the last chunk (monotonic key hotspot). Time-series collections are another option for raw points.

**"Why not one update per event for state?"** A batch has many events per vehicle; we only write the newest one per vehicle (`TelemetryProcessor`), which cuts writes a lot.

## Kubernetes

- **Probes**: startup (slow JVM start), liveness (restart if wedged), readiness (out of the Service while not ready). Backed by Spring Boot's `/actuator/health/liveness|readiness`.
- **Graceful shutdown**: `preStop` sleep + `terminationGracePeriodSeconds: 45`; Spring stops the listener container, the current batch finishes and its offsets commit.
- **HPA** on CPU 70%, 3 to 12 replicas; **PDB** keeps at least 2 pods during node drains.
- **Resources**: requests for scheduling, memory limit to protect the node; `-XX:MaxRAMPercentage=75` sizes the heap from the container limit.
- **Config** via ConfigMap env vars; secrets would go in a Secret / external secret store.
- Manifests are validated in CI with `kubeconform`.

## Monitoring

- Micrometer counters for received/stored/duplicate/stale/alerts and a batch-duration timer with p95/p99, read from `/actuator/metrics`.
- `json` profile switches logs to JSON (Spring Boot 3.4 structured logging), one object per line for `kubectl logs`; MDC carries `batchSize`.

**"What would you alert on?"** Consumer lag growing for 5 minutes, DLT rate above zero, and p99 batch latency above the poll interval budget.

## Testing strategy

- Unit tests for the processor logic with Mockito.
- Embedded Kafka test through the real listener container and error handler (DLT, retry, permanent failure).
- Real MongoDB through Testcontainers for the query semantics mocks can't prove (E11000 behaviour, geo index, TTL index creation).

## What I'd add next

- Geofence rules (enter/exit polygons, `$geoIntersects`) and route-deviation, as in my SafeTrax work.
- Message versioning: a `schemaVersion` field on the JSON payload so old and new devices can share the topic.
- Replay tool for the DLT.
- KEDA autoscaling on consumer lag instead of CPU.
