package p2p;

 // The full set of message types this system exchanges over the wire.
 // CHAT is the only sequenced, causally-ordered message type - it's the one
 // that goes through DeliveryManager's per-channel hold-back queue. The rest
 // are control/liveness messages that never consume a channel sequence
 // number and never sit in the hold-back queue:
 //   JOIN      - "I just (re)started"; broadcast on startup, asks every peer
 //               to reply with a SYNC so this peer can catch up.
 //   SYNC      - direct reply to a JOIN, carrying the replier's current
 //               vector clock plus its bookkeeping for its channel with the
 //               joiner, so a peer that just restarted with a zeroed-out
 //               clock and channel counters can resume exactly where it
 //               left off instead of getting stuck forever - see
 //               DeliveryManager.buildControl() / adoptClock().
 //   HEARTBEAT - liveness only.
public enum MessageType {
    JOIN,
    CHAT,
    HEARTBEAT,
    SYNC
}