package p2p;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// Everything about SENDING messages to other peers lives here, so Cli and
// Peer don't need to know about sockets, delays, or DeliveryManager's
// send-side API directly.

// A broadcast calls DeliveryManager.prepareForBroadcast() exactly ONCE (one
// causal send event, one clock snapshot), which hands back a separate
// per-recipient Message for each target - each copy carries the same clock
// snapshot but its own sequence number on that recipient's channel (see
// DeliveryManager). It is one logical event fanned out to many recipients,
// not many separate events, even though each recipient now gets a distinct
// Message object.

// All actual socket I/O happens on a background executor so the CLI thread
// never blocks waiting for a slow or dead peer's connection attempt to
// time out.

final class OutboundRouter {

    private final DeliveryManager deliveryManager;
    private final Map<String, PeerInfo> otherPeersById;
    private final Map<String, Integer> delaySecondsByPeerId = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor;

    OutboundRouter(DeliveryManager deliveryManager, List<PeerInfo> otherPeers) {
        this.deliveryManager = deliveryManager;
        this.otherPeersById = otherPeers.stream()
                .collect(Collectors.toMap(PeerInfo::getId, p -> p, (a, b) -> a, LinkedHashMap::new));
        this.executor = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "outbound-sender");
            t.setDaemon(true);
            return t;
        });
    }


    // Sets an artificial delay (seconds) applied to every future send targeting this peer. 0 clears it.
    void setDelay(String peerId, int seconds) {
        requireKnownPeer(peerId);
        if (seconds <= 0) {
            delaySecondsByPeerId.remove(peerId);
        } else {
            delaySecondsByPeerId.put(peerId, seconds);
        }
    }

    int getDelay(String peerId) {
        return delaySecondsByPeerId.getOrDefault(peerId, 0);
    }

    boolean isKnownPeer(String peerId) {
        return otherPeersById.containsKey(peerId);
    }

    List<String> knownPeerIds() {
        return new ArrayList<>(otherPeersById.keySet());
    }

    private void requireKnownPeer(String peerId) {
        if (!isKnownPeer(peerId)) {
            throw new IllegalArgumentException("Unknown peer id: " + peerId
                    + " (known peers: " + knownPeerIds() + ")");
        }
    }

    // ---------- Sending ----------

    // Broadcasts a CHAT message to every other configured peer. Each
    // recipient gets its own copy of the Message, stamped with its own
    // channel sequence number - see DeliveryManager.prepareForBroadcast().
    void broadcastChat(String body) {
        broadcastToAll(MessageType.CHAT, body);
    }

    // Sends a CHAT message to exactly one named peer.
    void directChat(String targetId, String body) {
        requireKnownPeer(targetId);
        Message message = deliveryManager.prepareForSend(MessageType.CHAT, targetId, body);
        sendWithConfiguredDelay(otherPeersById.get(targetId), message);
    }

    private void broadcastToAll(MessageType type, String body) {
        List<String> receiverIds = new ArrayList<>(otherPeersById.keySet());
        List<Message> copies = deliveryManager.prepareForBroadcast(type, body, receiverIds);
        for (int i = 0; i < receiverIds.size(); i++) {
            sendWithConfiguredDelay(otherPeersById.get(receiverIds.get(i)), copies.get(i));
        }
    }

    void announceJoin() {
        Message join = deliveryManager.buildControl(MessageType.JOIN, null);
        for (PeerInfo target : otherPeersById.values()) {
            sendWithConfiguredDelay(target, join);
        }
    }

    void sendSync(String targetId) {
        PeerInfo target = otherPeersById.get(targetId);
        if (target == null) {
            return;
        }
        Message sync = deliveryManager.buildControl(MessageType.SYNC, targetId);
        sendWithConfiguredDelay(target, sync);
    }

    // Broadcasts a heartbeat to every other peer. Heartbeats bypass the
    // artificial delay setting (delay is for demonstrating causal ordering
    // of CHAT/JOIN content, not for testing failure detection) and failures
    // are logged only at a glance, not per-peer per-tick, since the
    // receiving side's FailureDetector is the real source of truth for
    // "is this peer still there".
    void broadcastHeartbeat() {
        Message heartbeat = deliveryManager.buildHeartbeat();
        for (PeerInfo target : otherPeersById.values()) {
            executor.submit(() -> {
                try {
                    Sender.send(target, heartbeat);
                } catch (IOException ignored) {
                    // Expected whenever a peer is down or not started yet - no need to log every tick.
                }
            });
        }
    }

    private void sendWithConfiguredDelay(PeerInfo target, Message message) {
        int delaySeconds = delaySecondsByPeerId.getOrDefault(target.getId(), 0);
        if (delaySeconds <= 0) {
            executor.submit(() -> sendAndLog(target, message));
        } else {
            PeerLog.log("DELAY", "Holding " + message.getType() + " to " + target.getId()
                    + " for " + delaySeconds + "s (artificial demo delay)");
            executor.schedule(() -> sendAndLog(target, message), delaySeconds, TimeUnit.SECONDS);
        }
    }

    private void sendAndLog(PeerInfo target, Message message) {
        try {
            Sender.send(target, message);
            PeerLog.log("SEND", message.getType() + " -> " + target.getId()
                    + (message.getBody().isEmpty() ? "" : "  \"" + message.getBody() + "\""));
        } catch (IOException e) {
            PeerLog.log("WARN", "Could not reach " + target.getId() + " (" + e.getMessage() + ") - is it running?");
        }
    }

    void shutdown() {
        executor.shutdownNow();
    }
}