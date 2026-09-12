package p2p;

 // The full set of message types this system exchanges over the wire.
 // Kept intentionally small: three types cover join announcements, actual
 // chat content (broadcast or direct), and liveness heartbeats.
public enum MessageType {
    JOIN,
    CHAT,
    HEARTBEAT
}
