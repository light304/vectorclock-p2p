package p2p;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

 // A vector clock tracks one counter per known peer ID, giving each peer a
 // partial-order view of "what has happened before what" across the whole
 // system, without needing a shared physical clock.
 //
 // This class is intentionally the ONLY place that vector-clock arithmetic
 // happens. Nothing outside p2p.DeliveryManager should ever mutate a clock
 // directly - see beforeSend() / onDeliver() there.
public class VectorClock {

    // entryCountByPeerId maps each known peer's ID to that peer's counter.
    private final Map<String, Integer> entryCountByPeerId;

    public VectorClock() {
        this.entryCountByPeerId = new HashMap<>();
    }

    // Creates a clock pre-seeded with 0 for every peer ID given (including self).
    public VectorClock(Iterable<String> knownPeerIds) {
        this.entryCountByPeerId = new HashMap<>();
        for (String peerId : knownPeerIds) {
            entryCountByPeerId.put(peerId, 0);
        }
    }

    // Builds a clock from an existing snapshot (used when a message arrives).
    public static VectorClock fromSnapshot(Map<String, Integer> snapshot) {
        VectorClock clock = new VectorClock();
        clock.entryCountByPeerId.putAll(snapshot);
        return clock;
    }

    // Returns this peer's counter for a given peer ID, treating unknown IDs as 0.
    public synchronized int get(String peerId) {
        return entryCountByPeerId.getOrDefault(peerId, 0);
    }

         // Increments the counter belonging to ownerId. Called exactly once for
     // every "event" that happens at a peer: sending a message, or delivering
     // one. This is what makes the clock a record of local progress.
    public synchronized void increment(String ownerId) {
        entryCountByPeerId.merge(ownerId, 1, Integer::sum);
    }

         // Merges another clock into this one by taking the entrywise maximum,
     // then returns this clock (for chaining). This is the "catch up to
     // everything the sender knew" step that happens on delivery.
    public synchronized VectorClock mergeWith(VectorClock other) {
        for (Map.Entry<String, Integer> entry : other.entryCountByPeerId.entrySet()) {
            entryCountByPeerId.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return this;
    }

    // Returns an immutable point-in-time copy of the counters, for attaching to outgoing messages.
    public synchronized Map<String, Integer> snapshot() {
        return Collections.unmodifiableMap(new TreeMap<>(entryCountByPeerId));
    }

    // How two clocks relate to each other in the causal partial order.
    public enum Relation {
        BEFORE,       // this happened-before other
        AFTER,        // this happened-after other
        CONCURRENT,   // neither happened-before the other - truly independent events
        EQUAL         // identical clocks
    }

         // Compares this clock against another. This is a general-purpose causal
     // comparison (useful for logging/demo output); the actual delivery
     // decision uses the more specific rule in DeliveryManager.isDeliverable(),
     // not this method.
    public synchronized Relation compareTo(VectorClock other) {
        boolean thisHasSomethingOtherLacks = false;
        boolean otherHasSomethingThisLacks = false;

        for (String peerId : allPeerIdsAcross(this, other)) {
            int thisCount = this.get(peerId);
            int otherCount = other.get(peerId);
            if (thisCount > otherCount) thisHasSomethingOtherLacks = true;
            if (otherCount > thisCount) otherHasSomethingThisLacks = true;
        }

        if (!thisHasSomethingOtherLacks && !otherHasSomethingThisLacks) return Relation.EQUAL;
        if (thisHasSomethingOtherLacks && !otherHasSomethingThisLacks) return Relation.AFTER;
        if (!thisHasSomethingOtherLacks && otherHasSomethingThisLacks) return Relation.BEFORE;
        return Relation.CONCURRENT;
    }

    private static Iterable<String> allPeerIdsAcross(VectorClock a, VectorClock b) {
        Map<String, Integer> combined = new HashMap<>(a.entryCountByPeerId);
        combined.putAll(b.entryCountByPeerId);
        return combined.keySet();
    }

    @Override
    public synchronized String toString() {
        return snapshot().toString();
    }
}
