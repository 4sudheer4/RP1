package com.rp1.testkit;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Single KRaft broker (apache/kafka:3.9.0, same image as compose) reachable from the host only through Toxiproxy:
 * <ul>
 *   <li>{@link #proxiedBootstrap()}: what services use; toxics on {@link #proxy} hit bootstrap and leader traffic</li>
 *   <li>{@link #directBootstrap()}: a second, never-toxic proxy for admin/verification</li>
 * </ul>
 * Both advertised addresses are Toxiproxy host ports, known before the broker starts, so no host-port mapping
 * of the broker is needed. Configured explicitly (like infra/docker-compose.yml) rather than via Testcontainers'
 * KafkaContainer, whose generated controller listener is rejected by Kafka 3.9's config validation.
 * Started once per JVM.
 */
public final class KafkaBehindToxiproxy {
    private static final Network NETWORK = Network.newNetwork();
    private static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0")
            .withNetwork(NETWORK);
    private static GenericContainer<?> kafka;
    /** Proxy carrying all service traffic; add/remove toxics or disable it in tests. */
    public static Proxy proxy;

    private KafkaBehindToxiproxy() {}

    public static synchronized void start(Map<String, Integer> topicPartitions) throws Exception {
        if (kafka != null) return;
        TOXIPROXY.start();
        ToxiproxyClient client = new ToxiproxyClient(host(), TOXIPROXY.getControlPort());
        proxy = client.createProxy("kafka", "0.0.0.0:8666", "kafka:19092");
        client.createProxy("kafka-direct", "0.0.0.0:8667", "kafka:19093");

        kafka = new GenericContainer<>("apache/kafka:3.9.0")
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("CLUSTER_ID", "rp1-testkit-cluster-0001")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                .withEnv("KAFKA_LISTENERS", "CONTROLLER://:9093,INTERNAL://:29092,PROXIED://:19092,DIRECT://:19093")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka:29092,PROXIED://" + proxiedBootstrap()
                        + ",DIRECT://" + directBootstrap())
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,PROXIED:PLAINTEXT,DIRECT:PLAINTEXT")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofSeconds(90)));
        kafka.start();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", directBootstrap()))) {
            List<NewTopic> topics = topicPartitions.entrySet().stream()
                    .map(e -> new NewTopic(e.getKey(), e.getValue(), (short) 1)).toList();
            admin.createTopics(topics).all().get();
        }
    }

    public static String proxiedBootstrap() {
        return host() + ":" + TOXIPROXY.getMappedPort(8666);
    }

    public static String directBootstrap() {
        return host() + ":" + TOXIPROXY.getMappedPort(8667);
    }

    /** Testcontainers can report 0.0.0.0 on Docker Desktop; Kafka refuses to advertise a non-routable address. */
    private static String host() {
        String h = TOXIPROXY.getHost();
        return "0.0.0.0".equals(h) ? "localhost" : h;
    }

    public static void resetToxics() throws Exception {
        proxy.enable();
        for (var t : proxy.toxics().getAll()) t.remove();
    }

    /** Reads everything currently in the topic through the direct (never-toxic) listener. */
    public static List<ConsumerRecord<String, byte[]>> readAll(String topic) {
        try (var c = new KafkaConsumer<String, byte[]>(Map.of(
                "bootstrap.servers", directBootstrap(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(topic));
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            long idleUntil = System.currentTimeMillis() + 4000;
            while (System.currentTimeMillis() < idleUntil) {
                var batch = c.poll(Duration.ofMillis(300));
                batch.forEach(out::add);
                if (!batch.isEmpty()) idleUntil = System.currentTimeMillis() + 1500;
            }
            return out;
        }
    }
}
