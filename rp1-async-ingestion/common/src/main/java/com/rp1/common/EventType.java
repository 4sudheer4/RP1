package com.rp1.common;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SFMC ENS event types (official "Supported Notification Events" list) and their place in the per-message
 * status state machine.
 *
 * <p>{@code rank} orders delivery states: a message can only move to a higher rank. Ties (two terminal states, or
 * repeated SmsTransient updates) are broken by ordering timestamp, then event id, so the projected status is a
 * max over a total order: the same set of events gives the same final state in any arrival order. Rank 0 =
 * engagement facts (open/click/unsubscribe) that are recorded but never change delivery status.
 */
public enum EventType {
    EMAIL_SENT("TransactionalSendEvents.EmailSent", Channel.EMAIL, 1, "SENT"),
    EMAIL_NOT_SENT("TransactionalSendEvents.EmailNotSent", Channel.EMAIL, 3, "NOT_SENT"),
    EMAIL_BOUNCED("TransactionalSendEvents.EmailBounced", Channel.EMAIL, 3, "BOUNCED"),
    SMS_SENT("TransactionalSendEvents.SmsSent", Channel.SMS, 1, "SENT"),
    SMS_TRANSIENT("TransactionalSendEvents.SmsTransient", Channel.SMS, 2, "TRANSIENT"),
    SMS_NOT_SENT("TransactionalSendEvents.SmsNotSent", Channel.SMS, 3, "NOT_SENT"),
    SMS_BOUNCED("TransactionalSendEvents.SmsBounced", Channel.SMS, 3, "BOUNCED"),
    SMS_DELIVERED("TransactionalSendEvents.SmsDelivered", Channel.SMS, 3, "DELIVERED"),
    EMAIL_OPEN("EngagementEvents.EmailOpen", Channel.EMAIL, 0, null),
    EMAIL_CLICK("EngagementEvents.EmailClick", Channel.EMAIL, 0, null),
    EMAIL_UNSUBSCRIBE("EngagementEvents.EmailUnsubscribe", Channel.EMAIL, 0, null);

    public enum Channel { EMAIL, SMS }

    private static final Map<String, EventType> BY_WIRE =
            Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(EventType::wireName, Function.identity()));

    private final String wireName;
    private final Channel channel;
    private final int rank;
    private final String status;

    EventType(String wireName, Channel channel, int rank, String status) {
        this.wireName = wireName;
        this.channel = channel;
        this.rank = rank;
        this.status = status;
    }

    public static Optional<EventType> fromWire(String eventCategoryType) {
        return Optional.ofNullable(eventCategoryType).map(BY_WIRE::get);
    }

    public String wireName() { return wireName; }
    public Channel channel() { return channel; }
    public int rank() { return rank; }
    /** Delivery status this event moves the message to, or null for engagement facts. */
    public String status() { return status; }
    public boolean affectsStatus() { return rank > 0; }
}
