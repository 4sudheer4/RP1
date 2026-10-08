package com.rp1.common;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class EnvelopeFactoryTest {
    private final EnvelopeFactory factory = new EnvelopeFactory(Duration.ofMinutes(5));
    private final Instant now = Instant.parse("2026-10-08T12:00:00Z");

    private static JsonNode json(String s) throws Exception {
        return Json.MAPPER.readTree(s);
    }

    private static final String SENT = """
            {"eventCategoryType":"TransactionalSendEvents.EmailSent","timestampUTC":1600698608530,
             "compositeId":"x.1","definitionKey":"d","mid":1,"eid":1,
             "info":{"to":"a@b.c","subscriberKey":"sub-1","messageKey":"m-1","status":"Sent"}}""";

    @Test
    void sameEventDifferentKeyOrderAndWhitespaceGetsSameId() throws Exception {
        JsonNode a = json(SENT);
        JsonNode b = json("""
                {"info":{"messageKey":"m-1","status":"Sent","subscriberKey":"sub-1","to":"a@b.c"},
                 "eid":1,"mid":1,"definitionKey":"d","compositeId":"x.1",
                 "timestampUTC":1600698608530,"eventCategoryType":"TransactionalSendEvents.EmailSent"}""");
        assertThat(EnvelopeFactory.eventId(a)).isEqualTo(EnvelopeFactory.eventId(b)).hasSize(64);
    }

    @Test
    void differentContentGetsDifferentId() throws Exception {
        JsonNode a = json(SENT);
        JsonNode b = json(SENT.replace("m-1", "m-2"));
        assertThat(EnvelopeFactory.eventId(a)).isNotEqualTo(EnvelopeFactory.eventId(b));
    }

    @Test
    void upstreamIdPreferredWhenPresent() throws Exception {
        JsonNode e = json("{\"eventId\":\"abc\",\"eventCategoryType\":\"x\"}");
        assertThat(EnvelopeFactory.eventId(e)).isEqualTo("u:abc");
    }

    @Test
    void validEventBuildsEnvelopeKeyedBySubscriber() throws Exception {
        var r = factory.build(json(SENT), "b1", now, "t1");
        assertThat(r).isInstanceOf(EnvelopeFactory.Valid.class);
        CallbackEnvelope env = ((EnvelopeFactory.Valid) r).envelope();
        assertThat(env.subscriberKey()).isEqualTo("sub-1");
        assertThat(env.messageKey()).isEqualTo("m-1");
        assertThat(env.upstreamTs()).isEqualTo(Instant.ofEpochMilli(1600698608530L));
        assertThat(env.orderingTs()).isEqualTo(env.upstreamTs());
        assertThat(env.clockSkewed()).isFalse();
        assertThat(env.schemaVersion()).isEqualTo(CallbackEnvelope.CURRENT_SCHEMA);
    }

    @Test
    void unknownExtraFieldsTolerated() throws Exception {
        var r = factory.build(json(SENT.replace("\"mid\":1", "\"mid\":1,\"brandNewField\":{\"x\":[1,2]}")), "b", now, "t");
        assertThat(r).isInstanceOf(EnvelopeFactory.Valid.class);
    }

    @Test
    void futureTimestampBeyondSkewIsClampedAndFlagged() throws Exception {
        long future = now.plus(Duration.ofHours(2)).toEpochMilli();
        var r = factory.build(json(SENT.replace("1600698608530", Long.toString(future))), "b", now, "t");
        CallbackEnvelope env = ((EnvelopeFactory.Valid) r).envelope();
        assertThat(env.clockSkewed()).isTrue();
        assertThat(env.orderingTs()).isEqualTo(now.plus(Duration.ofMinutes(5)));
        assertThat(env.upstreamTs()).isEqualTo(Instant.ofEpochMilli(future));
    }

    @Test
    void isoTimestampAcceptedAndGarbageFallsBackToReceivedAt() throws Exception {
        var iso = factory.build(json(SENT.replace("1600698608530", "\"2024-02-01T15:30:00.000Z\"")), "b", now, "t");
        assertThat(((EnvelopeFactory.Valid) iso).envelope().upstreamTs()).isEqualTo(Instant.parse("2024-02-01T15:30:00Z"));
        var bad = factory.build(json(SENT.replace("1600698608530", "\"yesterday\"")), "b", now, "t");
        assertThat(((EnvelopeFactory.Valid) bad).envelope().orderingTs()).isEqualTo(now);
    }

    @Test
    void invalidReasons() throws Exception {
        assertThat(reason("[1]")).isNull(); // arrays element-checked by caller; here: not an object
        assertThat(reason("42")).isEqualTo(EnvelopeFactory.InvalidReason.NOT_OBJECT);
        assertThat(reason("{\"info\":{}}")).isEqualTo(EnvelopeFactory.InvalidReason.MISSING_EVENT_TYPE);
        assertThat(reason("{\"eventCategoryType\":\"Foo.Bar\",\"info\":{\"subscriberKey\":\"s\"}}"))
                .isEqualTo(EnvelopeFactory.InvalidReason.UNSUPPORTED_EVENT_TYPE);
        assertThat(reason(SENT.replace("\"subscriberKey\":\"sub-1\",", "")))
                .isEqualTo(EnvelopeFactory.InvalidReason.MISSING_SUBSCRIBER_KEY);
        assertThat(reason(SENT.replace("\"messageKey\":\"m-1\",", "")))
                .isEqualTo(EnvelopeFactory.InvalidReason.MISSING_MESSAGE_KEY);
    }

    @Test
    void engagementEventDoesNotNeedMessageKey() throws Exception {
        var r = factory.build(json("{\"eventCategoryType\":\"EngagementEvents.EmailOpen\",\"info\":{\"subscriberKey\":\"s\"}}"),
                "b", now, "t");
        assertThat(r).isInstanceOf(EnvelopeFactory.Valid.class);
    }

    private EnvelopeFactory.InvalidReason reason(String s) throws Exception {
        JsonNode n = json(s);
        if (n.isArray()) return null;
        var r = factory.build(n, "b", now, "t");
        return r instanceof EnvelopeFactory.Invalid inv ? inv.reason() : null;
    }
}
