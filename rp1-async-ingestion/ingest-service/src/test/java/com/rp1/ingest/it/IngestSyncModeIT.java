package com.rp1.ingest.it;

import com.rp1.ingest.IngestApplication;
import com.rp1.ingest.TestBatches;
import com.rp1.ingest.engine.SignatureVerifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Sync baseline (feature flag = 'sync' profile): same endpoint and validation, work done inline, no Kafka. */
class IngestSyncModeIT {
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");
    static ConfigurableApplicationContext app;
    static String url;

    @BeforeAll
    static void boot() {
        PG.start();
        app = new SpringApplicationBuilder(IngestApplication.class).profiles("sync", "textlogs").run(
                "--server.port=0",
                "--spring.datasource.url=" + PG.getJdbcUrl(),
                "--spring.datasource.username=" + PG.getUsername(),
                "--spring.datasource.password=" + PG.getPassword(),
                "--rp1.ingest.signature.keys=" + TestBatches.KEY);
        url = "http://localhost:" + app.getEnvironment().getProperty("local.server.port") + "/callbacks";
    }

    @AfterAll
    static void stop() {
        if (app != null) app.close();
        PG.stop();
    }

    @Test
    void syncModeProcessesInlineAndDedupsRetriedBatch() throws Exception {
        String body = "[" + TestBatches.event("EmailSent", "sy-1", "sy-m1", 1) + ","
                + TestBatches.event("EmailBounced", "sy-1", "sy-m1", 2) + ","
                + "{\"eventCategoryType\":\"Nope\",\"info\":{\"subscriberKey\":\"x\"}}]";
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        var req = HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json")
                .header(SignatureVerifier.HEADER, SignatureVerifier.sign(b, TestBatches.KEY.getBytes(StandardCharsets.UTF_8)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(b)).build();
        HttpClient http = HttpClient.newHttpClient();
        assertThat(http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        assertThat(http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200); // SFMC retry

        JdbcTemplate jdbc = app.getBean(JdbcTemplate.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM callback_event WHERE subscriber_key = 'sy-1'", Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT status FROM message_status WHERE message_key = 'sy-m1'", String.class)).isEqualTo("BOUNCED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM invalid_event", Integer.class)).isEqualTo(2); // once per delivery
    }
}
