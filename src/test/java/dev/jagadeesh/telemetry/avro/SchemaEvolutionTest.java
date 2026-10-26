package dev.jagadeesh.telemetry.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure Avro, no Kafka: proves the v1 → v2 change is safe in both directions and that the two classic breaking
 * changes are caught. Schema Registry runs the same checks when a schema is registered (see the IT).
 */
class SchemaEvolutionTest {

    static final Schema V1 = load("/avro/gps_reading_v1.avsc");
    static final Schema V2 = GpsReading.getClassSchema();
    static final Instant TS = Instant.parse("2026-10-26T04:00:00Z");

    @Test
    void v2ReaderDecodesV1RecordsWithFuelDefaultedToNull() throws IOException {
        GenericRecord v1 = new GenericData.Record(V1);
        v1.put("eventId", "e-1");
        v1.put("vehicleId", "KA-0001");
        v1.put("ts", TS.toEpochMilli());
        v1.put("lat", 12.97);
        v1.put("lon", 77.59);
        v1.put("speedKph", 42.0);
        v1.put("heading", 90.0);

        var out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(V1).write(v1, enc);
        enc.flush();

        GpsReading read = new SpecificDatumReader<GpsReading>(V1, V2)
                .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        assertThat(read.getEventId()).isEqualTo("e-1");
        assertThat(read.getTs()).isEqualTo(TS);
        assertThat(read.getFuelPct()).isNull();
    }

    @Test
    void v1ReaderIgnoresTheNewFieldSoOldConsumersKeepWorking() throws IOException {
        GpsReading v2 = GpsReading.newBuilder().setEventId("e-2").setVehicleId("KA-0002").setTs(TS)
                .setLat(12.97).setLon(77.59).setSpeedKph(55).setHeading(180).setFuelPct(63.5).build();
        var out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<GpsReading>(V2).write(v2, enc);
        enc.flush();

        GenericRecord read = new GenericDatumReader<GenericRecord>(V2, V1)
                .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        assertThat(read.get("eventId").toString()).isEqualTo("e-2");
        assertThat(read.hasField("fuelPct")).isFalse();
    }

    @Test
    void v2IsBackwardAndForwardCompatibleWithV1() {
        assertThat(compat(V2, V1)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);  // new reader, old data
        assertThat(compat(V1, V2)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);  // old reader, new data
    }

    @Test
    void aNewFieldWithoutDefaultBreaksOldData() {
        Schema bad = withField(V2, """
                {"name": "odometerKm", "type": "double"}""");
        assertThat(compat(bad, V1)).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }

    @Test
    void changingAFieldTypeBreaksOldData() {
        Schema bad = new Schema.Parser().parse(V2.toString().replace(
                "{\"name\":\"lat\",\"type\":\"double\"}", "{\"name\":\"lat\",\"type\":\"string\"}"));
        assertThat(bad.getField("lat").schema().getType()).isEqualTo(Schema.Type.STRING);
        assertThat(compat(bad, V1)).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }

    static SchemaCompatibilityType compat(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType();
    }

    static Schema withField(Schema base, String fieldJson) {
        String json = base.toString();
        int end = json.lastIndexOf(']');
        return new Schema.Parser().parse(json.substring(0, end) + "," + fieldJson + json.substring(end));
    }

    static Schema load(String path) {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream(path)) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
