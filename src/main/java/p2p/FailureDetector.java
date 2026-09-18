package p2p;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

 // Tracks whether each other peer appears to still be alive, based purely on
 // "have we heard anything from them recently" (a chat message, a JOIN, or a
 // heartbeat all count - see Peer.java, which calls recordSeen() for every
 // message type it receives).
 //
 // A peer starts as UNKNOWN rather than ALIVE or SUSPECTED, and only gets a
 // real status once we've heard from it at least once. This matters because
 // peers can be started in any order: if peer-C hasn't launched yet, peer-A
 // should not falsely accuse it of having failed.
final class FailureDetector {

    // Lets other classes (e.g. Cli) react to suspicion/recovery without scraping log output.
    interface SuspicionListener {
        void onSuspected(String peerId);
        void onRecovered(String peerId);
    }

    private final Set<String> peerIds; // other peers - never includes self
    private final long timeoutMillis;
    private final long checkIntervalMillis;
    private final LongSupplier nowMillis;

    private final Map<String, Long> lastSeenMillis = new ConcurrentHashMap<>();
    private final Set<String> suspected = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<SuspicionListener> listeners = new CopyOnWriteArrayList<>();

    private ScheduledExecutorService scheduler;

    FailureDetector(Set<String> peerIds, long timeoutMillis, long checkIntervalMillis) {
        this(peerIds, timeoutMillis, checkIntervalMillis, System::currentTimeMillis);
    }

    // Package-private constructor with an injectable clock, so tests don't depend on real wall-clock timing.
    FailureDetector(Set<String> peerIds, long timeoutMillis, long checkIntervalMillis, LongSupplier nowMillis) {
        this.peerIds = new LinkedHashSet<>(peerIds);
        this.timeoutMillis = timeoutMillis;
        this.checkIntervalMillis = checkIntervalMillis;
        this.nowMillis = nowMillis;
    }

    void addListener(SuspicionListener listener) {
        listeners.add(listener);
    }

    // Call this whenever ANY message (heartbeat, chat, join) is received from peerId.
    synchronized void recordSeen(String peerId) {
        lastSeenMillis.put(peerId, nowMillis.getAsLong());
        if (suspected.remove(peerId)) {
            PeerLog.log("RECOVER", peerId + " is responding again");
            for (SuspicionListener listener : listeners) {
                listener.onRecovered(peerId);
            }
        }
    }

         // Scans for peers that have gone quiet for too long. Called on a
     // schedule via start(), but also callable directly from tests so
     // behaviour can be verified deterministically without real sleeps.
    synchronized void checkForTimeouts() {
        long now = nowMillis.getAsLong();
        for (String peerId : peerIds) {
            Long lastSeen = lastSeenMillis.get(peerId);
            if (lastSeen == null) {
                continue; // never heard from this peer at all - that's UNKNOWN, not a failure
            }
            long silentFor = now - lastSeen;
            if (silentFor > timeoutMillis && suspected.add(peerId)) {
                PeerLog.log("SUSPECT", peerId + " - no contact for " + silentFor + "ms (timeout " + timeoutMillis + "ms)");
                for (SuspicionListener listener : listeners) {
                    listener.onSuspected(peerId);
                }
            }
        }
    }

    // Starts the periodic background check. Safe to call once; not restartable after stop().
    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "failure-detector");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::checkForTimeouts, checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);
    }

    void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    // Human-readable status per peer, for the CLI's "peers" command.
    synchronized Map<String, String> statusSnapshot() {
        Map<String, String> status = new LinkedHashMap<>();
        long now = nowMillis.getAsLong();
        for (String peerId : peerIds) {
            if (suspected.contains(peerId)) {
                status.put(peerId, "SUSPECTED");
            } else if (lastSeenMillis.containsKey(peerId)) {
                long agoMillis = now - lastSeenMillis.get(peerId);
                status.put(peerId, String.format("ALIVE (%.1fs ago)", agoMillis / 1000.0));
            } else {
                status.put(peerId, "UNKNOWN (no contact yet)");
            }
        }
        return status;
    }
}
