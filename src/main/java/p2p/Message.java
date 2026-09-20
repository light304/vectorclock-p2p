package p2p;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

 // A single message exchanged between peers, always serialised as exactly one
 // line of JSON terminated by '\n' on the wire (see Sender / Server).
 //
 // Fields:
 // type          - JOIN, CHAT, or HEARTBEAT
 // senderId      - the peer that originated this message
 // targetId      - null for a broadcast, or a specific peer ID for a direct message
 // body          - free-text payload (chat content; empty for JOIN/HEARTBEAT)
 // clock         - a snapshot of the sender's vector clock at send time
 // channelSeq    - this message's sequence number on the senderId -> receiver
 //                 channel (see DeliveryManager). A broadcast is one causal
 //                 send event but fans out into one Message PER RECIPIENT,
 //                 each carrying its own channelSeq for that recipient's
 //                 channel - that's what lets a receiver's FIFO check ignore
 //                 messages the sender addressed to someone else entirely.
 //                 Meaningless for HEARTBEAT (never checked - see
 //                 DeliveryManager.onMessageReceived).
 // sentAtMillis  - wall-clock send time, for human-readable logging only
 // (NOT used for ordering - that is what the vector clock + channelSeq are for)
public final class Message {

    private final MessageType type;
    private final String senderId;
    private final String targetId; // nullable: null means broadcast
    private final String body;
    private final Map<String, Integer> clock;
    private final int channelSeq;
    private final long sentAtMillis;

    public Message(MessageType type, String senderId, String targetId, String body,
                   Map<String, Integer> clock, int channelSeq, long sentAtMillis) {
        this.type = Objects.requireNonNull(type, "type");
        this.senderId = Objects.requireNonNull(senderId, "senderId");
        this.targetId = targetId; // intentionally nullable
        this.body = body == null ? "" : body;
        this.clock = Collections.unmodifiableMap(clock);
        this.channelSeq = channelSeq;
        this.sentAtMillis = sentAtMillis;
    }

    // Legacy constructor, kept so code that doesn't care about per-channel
    // sequencing (JSON round-trip tests, the real-TCP smoke test, HEARTBEAT
    // construction) doesn't need to invent a channelSeq value. Stamps 0,
    // which is never a valid sequence number (channels start at 1), so it
    // reads clearly as "not meaningfully sequenced" wherever it shows up.
    public Message(MessageType type, String senderId, String targetId, String body,
                   Map<String, Integer> clock, long sentAtMillis) {
        this(type, senderId, targetId, body, clock, 0, sentAtMillis);
    }

    public MessageType getType() {
        return type;
    }

    public String getSenderId() {
        return senderId;
    }

    // Null means this message was broadcast to every peer, not sent to one specifically.
    public String getTargetId() {
        return targetId;
    }

    public boolean isBroadcast() {
        return targetId == null;
    }

    public boolean isAddressedTo(String peerId) {
        return isBroadcast() || targetId.equals(peerId);
    }

    public String getBody() {
        return body;
    }

    public Map<String, Integer> getClock() {
        return clock;
    }

    // This message's sequence number on the senderId -> receiver channel. See the class javadoc.
    public int getChannelSeq() {
        return channelSeq;
    }

    public long getSentAtMillis() {
        return sentAtMillis;
    }

    // Serialises this message to a single line of JSON, no trailing newline.
    public String toJsonLine() {
        StringBuilder json = new StringBuilder();
        json.append('{');
        json.append("\"type\":").append(JsonUtil.writeString(type.name())).append(',');
        json.append("\"senderId\":").append(JsonUtil.writeString(senderId)).append(',');
        json.append("\"targetId\":").append(targetId == null ? "null" : JsonUtil.writeString(targetId)).append(',');
        json.append("\"body\":").append(JsonUtil.writeString(body)).append(',');
        json.append("\"clock\":").append(JsonUtil.writeIntMap(clock)).append(',');
        json.append("\"channelSeq\":").append(channelSeq).append(',');
        json.append("\"sentAtMillis\":").append(sentAtMillis);
        json.append('}');
        return json.toString();
    }

    // Parses a single line of JSON (as written by toJsonLine) back into a Message.
    @SuppressWarnings("unchecked")
    public static Message fromJsonLine(String jsonLine) {
        Map<String, Object> fields = JsonUtil.parseObject(jsonLine);

        String typeText = (String) requireField(fields, "type");
        MessageType type;
        try {
            type = MessageType.valueOf(typeText);
        } catch (IllegalArgumentException e) {
            throw new JsonUtil.JsonParseException("Unknown message type: " + typeText);
        }

        String senderId = (String) requireField(fields, "senderId");
        String targetId = (String) fields.get("targetId"); // may legitimately be null
        String body = (String) fields.getOrDefault("body", "");

        Object clockField = requireField(fields, "clock");
        if (!(clockField instanceof Map)) {
            throw new JsonUtil.JsonParseException("Expected 'clock' to be an object");
        }
        Map<String, Integer> clock = (Map<String, Integer>) clockField;

        int channelSeq;
        try {
            channelSeq = Integer.parseInt((String) requireField(fields, "channelSeq"));
        } catch (NumberFormatException e) {
            throw new JsonUtil.JsonParseException("Invalid channelSeq value");
        }

        long sentAtMillis;
        try {
            sentAtMillis = Long.parseLong((String) requireField(fields, "sentAtMillis"));
        } catch (NumberFormatException e) {
            throw new JsonUtil.JsonParseException("Invalid sentAtMillis value");
        }

        return new Message(type, senderId, targetId, body, clock, channelSeq, sentAtMillis);
    }

    private static Object requireField(Map<String, Object> fields, String key) {
        if (!fields.containsKey(key)) {
            throw new JsonUtil.JsonParseException("Missing required field '" + key + "'");
        }
        return fields.get(key);
    }

    @Override
    public String toString() {
        return toJsonLine();
    }
}