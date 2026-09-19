package p2p;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

 // The entry point. Wires every other class together into one running peer:
 //
 // PeerConfig      -> loads peers.json
 // DeliveryManager -> owns the vector clock, causal delivery, hold-back queue
 // FailureDetector -> tracks ALIVE / SUSPECTED / UNKNOWN per other peer
 // OutboundRouter  -> sends broadcasts, direct messages, JOIN, heartbeats
 // Server          -> listens for incoming connections
 // Cli             -> the interactive command loop (blocks on the main thread)
 //
 // Usage:
 // java p2p.Peer <path-to-peers.json> <this-peer's-id>
public final class Peer {

    private static final long HEARTBEAT_INTERVAL_SECONDS = 2;
    private static final long FAILURE_TIMEOUT_MILLIS = 6000;      // ~3 missed heartbeats
    private static final long FAILURE_CHECK_INTERVAL_MILLIS = 2000;

    private final String selfId;
    private final DeliveryManager deliveryManager;
    private final FailureDetector failureDetector;
    private final OutboundRouter outboundRouter;
    private final Server server;
    private final Cli cli;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private ScheduledExecutorService heartbeatScheduler;

    private Peer(String selfId, List<PeerInfo> allPeers, PeerInfo self) {
        this.selfId = selfId;

        Set<String> allPeerIds = allPeers.stream()
                .map(PeerInfo::getId)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<PeerInfo> others = allPeers.stream()
                .filter(p -> !p.getId().equals(selfId))
                .collect(Collectors.toList());
        Set<String> otherIds = others.stream()
                .map(PeerInfo::getId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        this.deliveryManager = new DeliveryManager(selfId, allPeerIds);
        this.failureDetector = new FailureDetector(otherIds, FAILURE_TIMEOUT_MILLIS, FAILURE_CHECK_INTERVAL_MILLIS);
        this.outboundRouter = new OutboundRouter(deliveryManager, others);
        this.server = new Server(self.getPort(), Math.max(4, others.size() * 2), this::handleIncoming);

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        this.cli = new Cli(selfId, deliveryManager, failureDetector, outboundRouter, stdin,
                this::onCrashCommand, this::onQuitCommand);

        deliveryManager.addListener(cli);
        failureDetector.addListener(cli);
    }

    // Routes every incoming message to both the failure detector (liveness) and the delivery manager (content/ordering).
    private void handleIncoming(Message message) {
        if (!message.getSenderId().equals(selfId)) { // defensive: should never happen, but never trust the wire
            failureDetector.recordSeen(message.getSenderId());
        }
        if (message.getType() == MessageType.HEARTBEAT) {
            // Heartbeats bypass DeliveryManager entirely - they're a liveness
            // signal, not causally-ordered content. The CLI owns whether they
            // get shown live or held back until the user asks for them.
            cli.onHeartbeat(message.getSenderId());
            return;
        }
        deliveryManager.onMessageReceived(message);
    }

    private void start() throws IOException {
        server.start();
        failureDetector.start();

        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeatScheduler.scheduleAtFixedRate(outboundRouter::broadcastHeartbeat,
                HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        // Best-effort: peers that haven't started yet will simply miss this. Heartbeats
        // reconcile everyone's ALIVE status naturally once all peers are up - no retry needed.
        outboundRouter.announceJoin();

        cli.run(); // blocks here until EOF (Ctrl+D) or a quit/crash command

        shutdown("stdin closed"); // reached if the user hits Ctrl+D instead of typing quit/crash
    }

    private void onCrashCommand() {
        shutdown("simulated crash");
        System.exit(0);
    }

    private void onQuitCommand() {
        shutdown("intentional shutdown");
        System.exit(0);
    }

    // Idempotent - safe to call more than once (e.g. from both a CLI command and stdin closing).
    private void shutdown(String reason) {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        PeerLog.log("SHUTDOWN", selfId + " stopping (" + reason + ")");
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdownNow();
        }
        failureDetector.stop();
        server.stop();
        outboundRouter.shutdown();
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java p2p.Peer <path-to-peers.json> <this-peer's-id>");
            System.exit(1);
            return;
        }

        Path configPath = Path.of(args[0]);
        String selfId = args[1];

        List<PeerInfo> allPeers;
        try {
            allPeers = PeerConfig.load(configPath);
        } catch (IOException e) {
            System.err.println("Failed to load peer config from " + configPath + ": " + e.getMessage());
            System.exit(1);
            return;
        }

        PeerInfo self = allPeers.stream()
                .filter(p -> p.getId().equals(selfId))
                .findFirst()
                .orElse(null);

        if (self == null) {
            String knownIds = allPeers.stream().map(PeerInfo::getId).collect(Collectors.joining(", "));
            System.err.println("Peer id '" + selfId + "' was not found in " + configPath + ". Known ids: " + knownIds);
            System.exit(1);
            return;
        }

        Peer peer = new Peer(selfId, allPeers, self);
        try {
            peer.start();
        } catch (IOException e) {
            System.err.println("Failed to start peer '" + selfId + "': " + e.getMessage());
            System.exit(1);
        }
    }
}
