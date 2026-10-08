package com.rp1.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventTypeTest {
    @Test
    void wireNamesRoundTrip() {
        for (EventType t : EventType.values()) assertThat(EventType.fromWire(t.wireName())).contains(t);
        assertThat(EventType.fromWire("TransactionalSendEvents.Nope")).isEmpty();
        assertThat(EventType.fromWire(null)).isEmpty();
    }

    @Test
    void sentRanksBelowEveryTerminalState() {
        assertThat(EventType.EMAIL_SENT.rank()).isLessThan(EventType.EMAIL_BOUNCED.rank());
        assertThat(EventType.SMS_SENT.rank()).isLessThan(EventType.SMS_TRANSIENT.rank());
        assertThat(EventType.SMS_TRANSIENT.rank()).isLessThan(EventType.SMS_DELIVERED.rank());
        assertThat(EventType.EMAIL_OPEN.affectsStatus()).isFalse();
    }
}
