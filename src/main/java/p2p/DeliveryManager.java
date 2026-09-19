package p2p;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

 // DeliveryManager is the SOLE owner of this peer's local vector clock. Every
 // other class (Server, Cli, etc.) goes through here rather than touching a
 // VectorClock directly - that's what keeps the clock arithmetic correct
 // under concurrent access, since every public method here is synchronized.
 //
 // It implements two things:
 // 1. beforeSend() / prepareForSend() - stamps outgoing messages with a
 // clock snapshot and advances the local clock for the send event.
 // 2. onMessageReceived() - the causal delivery rule. A message is only
 // "delivered" (merged into the clock, handed to listeners, logged as
 // DELIVER) once it's safe to do so; otherwise it sits in a hold-back
 // queue and is re-checked every time something new gets delivered.
 //
 // HEARTBEAT messages are treated as liveness-only: their clock is merged in
 // (so clocks still propagate even between chat messages) but they never go
 // through the hold-back queue and never trigger a DeliveryListener callback -
 // they aren't "content" a user needs to see delivered in order.
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

         // Call this immediately before sending a CHAT or JOIN message. Advances
     // this peer's own clock entry (a send is a local event) and returns a
     // fully-formed Message carrying a snapshot of the clock at this moment.
    public synchronized Message prepareForSend(MessageType type, String targetId, String body) {
        localClock.increment(selfId);
        Message message = new Message(type, selfId, targetId, body, localClock.snapshot(), System.currentTimeMillis());
        remember("SEND " + type + (targetId == null ? " (broadcast)" : " -> " + targetId));
        return message;
    }

         // Builds a HEARTBEAT message carrying a snapshot of the current clock for
     // logging purposes only. Deliberately does NOT increment the local
     // clock - heartbeats are a liveness signal, not a causally-ordered event.
    public synchronized Message buildHeartbeat() {
        return new Message(MessageType.HEARTBEAT, selfId, null, "", localClock.snapshot(), System.currentTimeMillis());
    }

    // ---------- Receiving ----------

    // Call this for every message that arrives over the wire, addressed to
    // this peer (broadcast or direct - routing is Server's job, not this
    // class's). Heartbeats never reach here - Peer.java routes them straight
    // to the CLI's heartbeat display (see Peer.handleIncoming()), since they
    // are a liveness signal, not a causally-ordered event. The guard below
    // is defensive only, in case that routing ever changes.
    public synchronized void onMessageReceived(Message incoming) {
        if (incoming.getType() == MessageType.HEARTBEAT) {
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

         // The causal delivery rule. A message from senderId is deliverable here
     // if and only if:
     // 1. it is exactly the next message we expect from that sender, and
     // 2. we already know everything the sender knew when they sent it.
    private boolean isDeliverable(Message incoming) {
        String sender = incoming.getSenderId();

        int expectedFromSender = localClock.get(sender) + 1;
        int actualFromSender = incoming.getClock().getOrDefault(sender, 0);
        if (actualFromSender != expectedFromSender) {
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
        int expected = localClock.get(sender) + 1;
        int actual = incoming.getClock().getOrDefault(sender, 0);

        if (actual != expected) {
            return incoming.getType() + " from " + sender + " -> waiting on earlier message from "
                    + sender + " (need " + sender + ":" + expected + ", got " + sender + ":" + actual + ")";
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
