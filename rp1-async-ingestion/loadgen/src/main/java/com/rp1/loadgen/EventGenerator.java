package com.rp1.loadgen;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rp1.common.EventType;
import com.rp1.common.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Produces SFMC-shaped events (field names from the official "Supported Notification Events" examples) that
 * follow realistic per-message lifecycles: email Sent[->Bounced|Open], NotSent; SMS Sent->Transient->Delivered,
 * Sent->Bounced. Many messages are active at once and their events interleave across batches, so a message's
 * events land in different HTTP requests and can be retried/delivered out of order, as in production.
 */
final class EventGenerator {

    record Generated(ObjectNode event, boolean valid, String messageKey, String subscriberKey, EventType type) {}

    private static final class Message {
        final String key;
        final String subscriber;
        final List<EventType> plan;
        int next;
        long lastTs;
        Message(String key, String subscriber, List<EventType> plan) {
            this.key = key;
            this.subscriber = subscriber;
            this.plan = plan;
        }
    }

    private final Random rnd;
    private final String runId;
    private final int subscribers;
    private final double hotFraction;
    private final double invalidFraction;
    private int poisonRemaining;
    private final List<Message> active = new ArrayList<>();
    private long messageSeq;
    private long invalidSeq;

    EventGenerator(long seed, String runId, int subscribers, int activeMessages, double hotFraction,
                   double invalidFraction, int poisonCount) {
        this.rnd = new Random(seed);
        this.runId = runId;
        this.subscribers = subscribers;
        this.hotFraction = hotFraction;
        this.invalidFraction = invalidFraction;
        this.poisonRemaining = poisonCount;
        for (int i = 0; i < activeMessages; i++) active.add(newMessage());
    }

    private Message newMessage() {
        String sub;
        if (poisonRemaining > 0 && rnd.nextInt(200) == 0) {
            poisonRemaining--;
            sub = "poison-" + runId + "-" + messageSeq;
        } else if (hotFraction > 0 && rnd.nextDouble() < hotFraction) {
            sub = runId + "-sub-hot";
        } else {
            sub = runId + "-sub-" + rnd.nextInt(subscribers);
        }
        return new Message(runId + "-msg-" + (messageSeq++), sub, plan());
    }

    private List<EventType> plan() {
        int p = rnd.nextInt(100);
        if (rnd.nextBoolean()) { // email
            if (p < 70) return List.of(EventType.EMAIL_SENT);
            if (p < 90) return List.of(EventType.EMAIL_SENT, EventType.EMAIL_BOUNCED);
            if (p < 95) return List.of(EventType.EMAIL_NOT_SENT);
            return List.of(EventType.EMAIL_SENT, EventType.EMAIL_OPEN);
        }
        if (p < 80) return List.of(EventType.SMS_SENT, EventType.SMS_TRANSIENT, EventType.SMS_DELIVERED);
        return List.of(EventType.SMS_SENT, EventType.SMS_BOUNCED);
    }

    Generated next() {
        if (invalidFraction > 0 && rnd.nextDouble() < invalidFraction) {
            ObjectNode bad = Json.MAPPER.createObjectNode();
            bad.put("eventCategoryType", "TransactionalSendEvents.EmailSent");
            bad.put("timestampUTC", System.currentTimeMillis());
            bad.putObject("info").put("messageKey", runId + "-invalid-" + (invalidSeq++)); // no subscriberKey -> invalid
            return new Generated(bad, false, null, null, EventType.EMAIL_SENT);
        }
        int i = rnd.nextInt(active.size());
        Message m = active.get(i);
        EventType type = m.plan.get(m.next++);
        if (m.next == m.plan.size()) active.set(i, newMessage());

        ObjectNode e = Json.MAPPER.createObjectNode();
        e.put("eventCategoryType", type.wireName());
        // epoch millis, strictly increasing per message: later lifecycle events always carry later timestamps
        m.lastTs = Math.max(System.currentTimeMillis(), m.lastTs + 1);
        e.put("timestampUTC", m.lastTs);
        e.put("compositeId", m.key + "." + m.next);
        e.put("definitionKey", "rp1-loadgen");
        e.put("mid", 1447640);
        e.put("eid", 1447640);
        ObjectNode info = e.putObject("info");
        info.put("to", type.channel() == EventType.Channel.EMAIL ? m.subscriber + "@example.test" : "+15550000000");
        info.put("subscriberKey", m.subscriber);
        info.put("messageKey", m.key);
        if (type.status() != null) info.put("status", type.status());
        return new Generated(e, true, m.key, m.subscriber, type);
    }
}
