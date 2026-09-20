package p2p;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

// DeliveryManager is the SOLE owner of this peer's local vector clock. Every
// other class (Server, Cli, etc.) goes through here rather than touching a
// VectorClock directly - that's what keeps the clock arithmetic correct
// under concurrent access, since every public method here is synchronized.

// It implements two things:
// 1. prepareForSend() / prepareForBroadcast() - stamp outgoing messages
// with a clock snapshot and a per-channel sequence number, advancing the
// local clock for the send event.
// 2. onMessageReceived() - the causal delivery rule. A message is only
// "delivered" (merged into the clock, handed to listeners, logged as
// DELIVER) once it's safe to do so; otherwise it sits in a hold-back
// queue and is re-checked every time something new gets delivered.

// Ordering is now PER CHANNEL rather than per sender. A "channel" is one
// sender-receiver pair (selfId -> some other peer, for sending; some other
// peer -> selfId, for receiving). Every message sent on a channel - whether
// it's one recipient's copy of a broadcast or a genuine direct message -
// takes the next number on that specific channel. A receiver's FIFO check
// only cares about the channel it's on the receiving end of, so a direct
// message the sender addressed to someone else entirely never blocks this
// peer's channel from that sender. The vector clock still does its
// original job of tracking cross-sender causal dependencies (via
// mergeWith() on delivery) - it's just no longer used for the sender's own
// FIFO ordering, which the channel sequence numbers now handle instead.

// HEARTBEAT messages are treated as liveness-only: they never carry a
// meaningful channelSeq, are never checked against it, and never go through
// the hold-back queue or trigger a DeliveryListener callback - they aren't
// "content" a user needs to see delivered in order.

public class DeliveryManager {

    // Something that wants to know when a CHAT/JOIN message is actually delivered, in causal order.
    public interface DeliveryListener {
        void onDelivered(Message message);
    }

    private final String selfId;
    private final Set<String> allPeerIds; // includes selfId
    private final VectorClock localClock;
    private final List<Message> holdBackQueue = new ArrayList<>();
    private final Deque<String> recentActivity = new ArrayDeque<>();
    private final List<DeliveryListener> listeners = new CopyOnWriteArrayList<>();

    // Outbound: last sequence number stamped on the selfId -> receiverId channel, per receiver.
    private final Map<String, Integer> outgoingChannelSeq = new HashMap<>();
    // Inbound: last sequence number actually delivered on the senderId -> selfId channel, per sender.
    private final Map<String, Integer> incomingChannelSeq = new HashMap<>();

    private static final int RECENT_ACTIVITY_LIMIT = 200;

    public DeliveryManager(String selfId, Set<String> allPeerIds) {
        if (!allPeerIds.contains(selfId)) {
            throw new IllegalArgumentException("allPeerIds must include selfId (" + selfId + ")");
        }
        this.selfId = selfId;
        this.allPeerIds = allPeerIds;
        this.localClock = new VectorClock(allPeerIds);
    }

    public void addListener(DeliveryListener listener) {
        listeners.add(listener);
    }

    // ---------- Sending ----------

    // Call this immediately before sending a CHAT message to exactly one
    // named recipient (a direct message - never a broadcast; use
    // prepareForBroadcast() for that). Advances this peer's own clock entry
    // (a send is a local event) and the selfId -> targetId channel's
    // sequence number, and returns a fully-formed Message carrying both.
    public synchronized Message prepareForSend(MessageType type, String targetId, String body) {
        if (targetId == null) {
            throw new IllegalArgumentException(
                    "prepareForSend requires a specific targetId; use prepareForBroadcast() for broadcasts");
        }
        localClock.increment(selfId);
        Map<String, Integer> snapshot = localClock.snapshot();
        int seq = nextChannelSeq(targetId);
        Message message = new Message(type, selfId, targetId, body, snapshot, seq, System.currentTimeMillis());
        remember("SEND " + type + " -> " + targetId);
        return message;
    }

    // Call this immediately before broadcasting a CHAT or JOIN message to
    // every peer in receiverIds. A broadcast is ONE causal send event - the
    // local clock advances exactly once, and every recipient's copy carries
    // the same clock snapshot - but each recipient's copy is stamped with
    // its OWN channel sequence number, since each selfId -> receiverId
    // channel has its own independent history. Returns one Message per
    // receiver, in the same order as receiverIds.
    public synchronized List<Message> prepareForBroadcast(MessageType type, String body, List<String> receiverIds) {
        localClock.increment(selfId);
        Map<String, Integer> snapshot = localClock.snapshot();
        long sentAtMillis = System.currentTimeMillis();

        List<Message> copies = new ArrayList<>(receiverIds.size());
        for (String receiverId : receiverIds) {
            int seq = nextChannelSeq(receiverId);
            copies.add(new Message(type, selfId, null, body, snapshot, seq, sentAtMillis));
        }
        remember("SEND " + type + " (broadcast)");
        return copies;
    }

    private int nextChannelSeq(String receiverId) {
        return outgoingChannelSeq.merge(receiverId, 1, Integer::sum);
    }

    // Builds a HEARTBEAT message carrying a snapshot of the current clock for
    // logging purposes only. Deliberately does NOT increment the local
    // clock and does NOT touch any channel counter - heartbeats are a
    // liveness signal, not a causally-ordered event, and are exempt from
    // channel sequencing entirely (see onMessageReceived()).
    public synchronized Message buildHeartbeat() {
        return new Message(MessageType.HEARTBEAT, selfId, null, "", localClock.snapshot(), System.currentTimeMillis());
    }

    // Builds a JOIN or SYNC control message. Like a heartbeat, neither ticks
    // the local clock nor consumes a channel sequence number - control
    // messages sit outside the per-channel sequence entirely, so a JOIN from
    // a freshly-restarted peer (whose channel counters reset to zero) can
    // never leave a gap that wedges anyone's hold-back queue.
    //
    // A SYNC's body carries "outToTarget:inFromTarget" - this peer's own
    // channel bookkeeping for its channel with targetId:
    //   outToTarget  = last seq WE have stamped on the selfId -> targetId channel
    //   inFromTarget = last seq WE have actually delivered on the targetId -> selfId channel
    // The rejoining peer on the other end uses these to resume both of its
    // channel counters with us right where they left off - see adoptClock().
    public synchronized Message buildControl(MessageType type, String targetId) {
        if (type != MessageType.JOIN && type != MessageType.SYNC) {
            throw new IllegalArgumentException("buildControl only builds JOIN or SYNC, not " + type);
        }
        String body = "";
        if (type == MessageType.SYNC) {
            int outToTarget = outgoingChannelSeq.getOrDefault(targetId, 0);
            int inFromTarget = incomingChannelSeq.getOrDefault(targetId, 0);
            body = outToTarget + ":" + inFromTarget;
        }
        Message message = new Message(type, selfId, targetId, body, localClock.snapshot(), System.currentTimeMillis());
        remember("SEND " + type + (targetId == null ? " (broadcast)" : " -> " + targetId));
        return message;
    }

    public synchronized void onMessageReceived(Message incoming) {
        if (incoming.getType() == MessageType.HEARTBEAT) {
            return;
        }
        if (incoming.getType() == MessageType.JOIN) {
            handleJoin(incoming);
            return;
        }
        if (incoming.getType() == MessageType.SYNC) {
            adoptClock(incoming);
            return;
        }

        if (isDeliverable(incoming)) {
            deliver(incoming);
            drainHoldBackQueue();
        } else {
            holdBackQueue.add(incoming);
            PeerLog.log("BUFFER", describeWhyBuffered(incoming));
            remember("BUFFER " + incoming.getType() + " from " + incoming.getSenderId());
        }
    }

    private void handleJoin(Message join) {
        PeerLog.log("DELIVER", "JOIN from " + join.getSenderId() + " (broadcast)");
        remember("DELIVER JOIN from " + join.getSenderId());
        for (DeliveryListener listener : listeners) {
            listener.onDelivered(join);
        }
    }

    public synchronized void adoptClock(Message sync) {
        String fromPeerId = sync.getSenderId();

        Map<String, Integer> before = localClock.snapshot();
        localClock.mergeWith(VectorClock.fromSnapshot(sync.getClock()));
        Map<String, Integer> after = localClock.snapshot();

        restoreChannelPositions(fromPeerId, sync.getBody());

        if (before.equals(after)) {
            PeerLog.log("CATCHUP", "clock from " + fromPeerId + " - already up to date  clock=" + after);
        } else {
            PeerLog.log("CATCHUP", "synced with " + fromPeerId + "  " + before + " -> " + after);
        }
        remember("CATCHUP from " + fromPeerId);

        int dropped = discardCoveredMessages(fromPeerId);
        if (dropped > 0) {
            PeerLog.log("CATCHUP", "discarded " + dropped + " buffered message(s) already covered by the sync");
        }
        drainHoldBackQueue();
    }

    private void restoreChannelPositions(String fromPeerId, String body) {
        String[] parts = body.split(":");
        if (parts.length != 2) {
            return; // not a SYNC body (e.g. empty, from a JOIN) - nothing to restore
        }
        try {
            int theirOutToUs = Integer.parseInt(parts[0]);
            int theirInFromUs = Integer.parseInt(parts[1]);
            incomingChannelSeq.merge(fromPeerId, theirOutToUs, Math::max);
            outgoingChannelSeq.merge(fromPeerId, theirInFromUs, Math::max);
        } catch (NumberFormatException ignored) {
            // malformed - ignore rather than crash the peer over a bad SYNC
        }
    }

    private int discardCoveredMessages(String fromPeerId) {
        int removed = 0;
        int haveSeq = incomingChannelSeq.getOrDefault(fromPeerId, 0);
        for (Message queued : new ArrayList<>(holdBackQueue)) {
            if (queued.getSenderId().equals(fromPeerId) && queued.getChannelSeq() <= haveSeq) {
                holdBackQueue.remove(queued);
                remember("DISCARD " + queued.getType() + " from " + fromPeerId + " (covered by catch-up)");
                removed++;
            }
        }
        return removed;
    }

    // The causal delivery rule. A message from senderId is deliverable here
    // if and only if:
    // 1. it is exactly the next message on the senderId -> selfId channel, and
    // 2. we already know everything the sender knew about every OTHER peer when they sent it.
    private boolean isDeliverable(Message incoming) {
        String sender = incoming.getSenderId();

        int expectedOnChannel = incomingChannelSeq.getOrDefault(sender, 0) + 1;
        if (incoming.getChannelSeq() != expectedOnChannel) {
            return false;
        }

        for (String otherPeerId : allPeerIds) {
            if (otherPeerId.equals(sender)) {
                continue;
            }
            int senderKnewAbout = incoming.getClock().getOrDefault(otherPeerId, 0);
            int weKnowAbout = localClock.get(otherPeerId);
            if (senderKnewAbout > weKnowAbout) {
                return false; // sender had seen something from otherPeerId that we haven't seen yet
            }
        }

        return true;
    }

    private void deliver(Message message) {
        // Advance the channel counter to exactly what we just delivered
        // (isDeliverable already confirmed it's the next one on this channel).
        incomingChannelSeq.put(message.getSenderId(), message.getChannelSeq());

        // Merge only - do NOT increment our own entry here. Our own entry
        // must only ever advance on SEND, so that the number we stamp on our
        // next outgoing message matches exactly what a receiver is waiting
        // for. If delivery also incremented our own entry, a message could
        // get stuck in another peer's hold-back queue forever, waiting for a
        // sequence number that no outgoing message will ever carry.
        localClock.mergeWith(VectorClock.fromSnapshot(message.getClock()));

        String direction = message.isBroadcast() ? "(broadcast)" : "-> " + message.getTargetId();
        PeerLog.log("DELIVER", message.getType() + " from " + message.getSenderId() + " " + direction
                + "  clock=" + localClock.snapshot()
                + (message.getBody().isEmpty() ? "" : "  \"" + message.getBody() + "\""));
        remember("DELIVER " + message.getType() + " from " + message.getSenderId()
                + (message.getBody().isEmpty() ? "" : ": \"" + message.getBody() + "\""));

        for (DeliveryListener listener : listeners) {
            listener.onDelivered(message);
        }
    }

    // After any delivery, re-scan the hold-back queue since it may have unblocked something.
    private void drainHoldBackQueue() {
        boolean deliveredSomething = true;
        while (deliveredSomething) {
            deliveredSomething = false;
            for (Message candidate : new ArrayList<>(holdBackQueue)) {
                if (isDeliverable(candidate)) {
                    holdBackQueue.remove(candidate);
                    deliver(candidate);
                    deliveredSomething = true;
                    break; // restart the scan - state changed
                }
            }
        }
    }

    private String describeWhyBuffered(Message incoming) {
        String sender = incoming.getSenderId();
        int expected = incomingChannelSeq.getOrDefault(sender, 0) + 1;
        int actual = incoming.getChannelSeq();

        if (actual != expected) {
            return incoming.getType() + " from " + sender + " -> waiting on earlier message from "
                    + sender + " (need #" + expected + " on this channel, got #" + actual + ")";
        }
        for (String otherPeerId : allPeerIds) {
            if (otherPeerId.equals(sender)) continue;
            int senderKnewAbout = incoming.getClock().getOrDefault(otherPeerId, 0);
            int weKnowAbout = localClock.get(otherPeerId);
            if (senderKnewAbout > weKnowAbout) {
                return incoming.getType() + " from " + sender + " -> waiting on " + otherPeerId
                        + " (need " + otherPeerId + ":" + senderKnewAbout + ", have " + otherPeerId + ":" + weKnowAbout + ")";
            }
        }
        return incoming.getType() + " from " + sender + " -> buffered"; // shouldn't normally happen
    }

    private void remember(String activity) {
        recentActivity.addLast(activity);
        if (recentActivity.size() > RECENT_ACTIVITY_LIMIT) {
            recentActivity.removeFirst();
        }
    }

    // ---------- Read-only queries (for the CLI) ----------

    public synchronized java.util.Map<String, Integer> currentClockSnapshot() {
        return localClock.snapshot();
    }

    public synchronized List<Message> currentHoldBackQueueSnapshot() {
        return new ArrayList<>(holdBackQueue);
    }

    public synchronized List<String> recentActivitySnapshot() {
        return new ArrayList<>(recentActivity);
    }
}