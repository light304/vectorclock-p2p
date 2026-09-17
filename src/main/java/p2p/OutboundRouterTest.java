package p2p;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

 // Standalone harness for OutboundRouter. Uses real Server instances bound to
 // OS-assigned free ports (port 0) so tests never collide with each other or
 // with a real running peer. Run with: java p2p.OutboundRouterTest
public class OutboundRouterTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        testBroadcastReachesAllTargetsWithSameClockSnapshot();
        testDirectChatOnlyReachesNamedTarget();
        testUnknownPeerIsRejected();
        testDelaySettingPostponesDelivery();
        testUnreachablePeerDoesNotBlockOtherSends();

        System.out.println();
        System.out.println("---------------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("---------------------------------------------");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // Starts a Server on an OS-assigned port that records every message it receives. Caller must call stop().
    private static class RecordingServer {
        final Server server;
        final List<Message> received = new CopyOnWriteArrayList<>();

        RecordingServer() throws IOException {
            server = new Server(0, 2, received::add);
            server.start();
        }

        PeerInfo asPeerInfo(String id) {
            return new PeerInfo(id, "localhost", server.getBoundPort());
        }

        void stop() {
            server.stop();
        }
    }

    private static Set<String> peerIdSet(String... ids) {
        return new LinkedHashSet<>(Arrays.asList(ids));
    }

    private static boolean waitUntil(java.util.function.BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }

    private static void testBroadcastReachesAllTargetsWithSameClockSnapshot() throws Exception {
        RecordingServer serverB = new RecordingServer();
        RecordingServer serverC = new RecordingServer();
        try {
            DeliveryManager dm = new DeliveryManager("peer-A", peerIdSet("peer-A", "peer-B", "peer-C"));
            List<PeerInfo> others = List.of(serverB.asPeerInfo("peer-B"), serverC.asPeerInfo("peer-C"));
            OutboundRouter router = new OutboundRouter(dm, others);

            router.broadcastChat("hello everyone");

            boolean bothArrived = waitUntil(() -> !serverB.received.isEmpty() && !serverC.received.isEmpty(), 2000);
            check("broadcast: both targets received the message", bothArrived);
            check("broadcast: both copies carry an identical clock snapshot",
                    serverB.received.get(0).getClock().equals(serverC.received.get(0).getClock()));
            check("broadcast: body preserved", serverB.received.get(0).getBody().equals("hello everyone"));

            router.shutdown();
        } finally {
            serverB.stop();
            serverC.stop();
        }
    }

    private static void testDirectChatOnlyReachesNamedTarget() throws Exception {
        RecordingServer serverB = new RecordingServer();
        RecordingServer serverC = new RecordingServer();
        try {
            DeliveryManager dm = new DeliveryManager("peer-A", peerIdSet("peer-A", "peer-B", "peer-C"));
            List<PeerInfo> others = List.of(serverB.asPeerInfo("peer-B"), serverC.asPeerInfo("peer-C"));
            OutboundRouter router = new OutboundRouter(dm, others);

            router.directChat("peer-B", "just for you");

            boolean arrivedAtB = waitUntil(() -> !serverB.received.isEmpty(), 2000);
            check("direct: named target received it", arrivedAtB);

            Thread.sleep(300); // give a wrongly-broadcast copy a chance to arrive, if the bug existed
            check("direct: the other peer received nothing", serverC.received.isEmpty());

            router.shutdown();
        } finally {
            serverB.stop();
            serverC.stop();
        }
    }

    private static void testUnknownPeerIsRejected() {
        DeliveryManager dm = new DeliveryManager("peer-A", peerIdSet("peer-A", "peer-B"));
        OutboundRouter router = new OutboundRouter(dm, List.of(new PeerInfo("peer-B", "localhost", 1)));

        boolean threwOnDirectChat = false;
        try {
            router.directChat("peer-Z", "hi");
        } catch (IllegalArgumentException expected) {
            threwOnDirectChat = true;
        }
        check("unknown peer: directChat is rejected", threwOnDirectChat);

        boolean threwOnSetDelay = false;
        try {
            router.setDelay("peer-Z", 5);
        } catch (IllegalArgumentException expected) {
            threwOnSetDelay = true;
        }
        check("unknown peer: setDelay is rejected", threwOnSetDelay);

        router.shutdown();
    }

    private static void testDelaySettingPostponesDelivery() throws Exception {
        RecordingServer serverB = new RecordingServer();
        try {
            DeliveryManager dm = new DeliveryManager("peer-A", peerIdSet("peer-A", "peer-B"));
            OutboundRouter router = new OutboundRouter(dm, List.of(serverB.asPeerInfo("peer-B")));

            router.setDelay("peer-B", 1); // 1 second artificial delay
            check("delay: getDelay reflects what was set", router.getDelay("peer-B") == 1);

            router.directChat("peer-B", "delayed message");

            Thread.sleep(300);
            check("delay: message has NOT arrived yet after 300ms", serverB.received.isEmpty());

            boolean arrivedEventually = waitUntil(() -> !serverB.received.isEmpty(), 2000);
            check("delay: message DOES arrive after the delay elapses", arrivedEventually);

            router.setDelay("peer-B", 0); // clearing it
            check("delay: setting to 0 clears it", router.getDelay("peer-B") == 0);

            router.shutdown();
        } finally {
            serverB.stop();
        }
    }

    private static void testUnreachablePeerDoesNotBlockOtherSends() throws Exception {
        RecordingServer serverB = new RecordingServer();
        try {
            DeliveryManager dm = new DeliveryManager("peer-A", peerIdSet("peer-A", "peer-B", "peer-DOWN"));
            // peer-DOWN points at a port nothing is listening on.
            PeerInfo down = new PeerInfo("peer-DOWN", "localhost", 1);
            List<PeerInfo> others = List.of(serverB.asPeerInfo("peer-B"), down);
            OutboundRouter router = new OutboundRouter(dm, others);

            router.broadcastChat("hello despite one peer being down");

            boolean arrivedAtLiveTarget = waitUntil(() -> !serverB.received.isEmpty(), 2000);
            check("unreachable peer: the OTHER, reachable peer still receives the broadcast", arrivedAtLiveTarget);

            router.shutdown();
        } finally {
            serverB.stop();
        }
    }

    private static void check(String description, boolean condition) {
        if (condition) {
            passed++;
            System.out.println("  [PASS] " + description);
        } else {
            failed++;
            System.out.println("  [FAIL] " + description);
        }
    }
}
