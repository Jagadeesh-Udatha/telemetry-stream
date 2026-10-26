package dev.jagadeesh.telemetry.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Against a real Confluent Schema Registry (Testcontainers): the subject is set to BACKWARD, v1 and v2 register as
 * versions 1 and 2, and a breaking schema is rejected with HTTP 409 before any producer could use it.
 * This is the check CI runs before a schema change can ship. Skipped when Docker isn't available.
 */
@Testcontainers(disabledWithoutDocker = true)
class SchemaRegistryCompatibilityTest {

    static final String SUBJECT = "telemetry.raw.avro-value";
    static Network network;
    static ConfluentKafkaContainer kafka;
    static GenericContainer<?> registry;
    static CachedSchemaRegistryClient client;

    @BeforeAll
    static void start() {
        network = Network.newNetwork();
        kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
                .withNetwork(network).withListener("kafka:19092");
        kafka.start();
        registry = new GenericContainer<>("confluentinc/cp-schema-registry:7.8.0")
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        registry.start();
        client = new CachedSchemaRegistryClient("http://" + registry.getHost() + ":" + registry.getMappedPort(8081), 10);
    }

    @AfterAll
    static void stop() {
        if (registry != null) registry.stop();
        if (kafka != null) kafka.stop();
        if (network != null) network.close();
    }

    @Test
    void registryEnforcesBackwardCompatibility() throws Exception {
        client.updateCompatibility(SUBJECT, "BACKWARD");

        int v1Id = client.register(SUBJECT, new AvroSchema(SchemaEvolutionTest.V1));
        assertThat(client.testCompatibility(SUBJECT, new AvroSchema(SchemaEvolutionTest.V2))).isTrue();
        int v2Id = client.register(SUBJECT, new AvroSchema(SchemaEvolutionTest.V2));
        assertThat(v2Id).isNotEqualTo(v1Id);
        assertThat(client.getAllVersions(SUBJECT)).containsExactly(1, 2);

        Schema breaking = SchemaEvolutionTest.withField(SchemaEvolutionTest.V2, """
                {"name": "odometerKm", "type": "double"}""");
        assertThat(client.testCompatibility(SUBJECT, new AvroSchema(breaking))).isFalse();
        assertThatThrownBy(() -> client.register(SUBJECT, new AvroSchema(breaking)))
                .isInstanceOf(RestClientException.class)
                .satisfies(e -> assertThat(((RestClientException) e).getStatus()).isEqualTo(409));
    }
}
