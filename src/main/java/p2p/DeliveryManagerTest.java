package p2p;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

// Standalone harness for DeliveryManager. Simulates message exchange between
// several in-memory DeliveryManager instances (no sockets involved).
// Run with: java p2p.DeliveryManagerTest
public class DeliveryManagerTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        testInOrderDeliveryNeedsNoBuffering();
        testOutOfOrderMessageIsBufferedThenDelivered();
        testHeartbeatMergesWithoutTriggeringDelivery();

        System.out.println();
        System.out.println("---------------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("---------------------------------------------");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static Set<String> peers(String... ids) {
        return new LinkedHashSet<>(Arrays.asList(ids));
    }

    private static void testInOrderDeliveryNeedsNoBuffering() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        Message m1 = a.prepareForSend(MessageType.CHAT, null, "hello");
        b.onMessageReceived(m1);

        check("in-order: message delivered immediately (no buffering)",
                b.currentHoldBackQueueSnapshot().isEmpty());
        check("in-order: recipient's clock reflects the delivery",
                b.currentClockSnapshot().get("peer-A") == 1);
    }

    private static void testOutOfOrderMessageIsBufferedThenDelivered() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        Message first = a.prepareForSend(MessageType.CHAT, null, "first");
        Message second = a.prepareForSend(MessageType.CHAT, null, "second");

        b.onMessageReceived(second);
        check("out-of-order: second message is buffered, not delivered",
                b.currentHoldBackQueueSnapshot().size() == 1);
        check("out-of-order: clock unchanged while buffered",
                b.currentClockSnapshot().get("peer-A") == 0);

        b.onMessageReceived(first);
        check("out-of-order: once 'first' arrives, both are delivered (queue drains)",
                b.currentHoldBackQueueSnapshot().isEmpty());
        check("out-of-order: clock reflects both messages",
                b.currentClockSnapshot().get("peer-A") == 2);
    }

    private static void testHeartbeatMergesWithoutTriggeringDelivery() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        a.prepareForSend(MessageType.CHAT, null, "bump A's clock to 1");
        Message heartbeat = new Message(MessageType.HEARTBEAT, "peer-A", null, "",
                a.currentClockSnapshot(), System.currentTimeMillis());

        b.onMessageReceived(heartbeat);

        check("heartbeat: clock is merged in", b.currentClockSnapshot().get("peer-A") == 1);
        check("heartbeat: never enters the hold-back queue", b.currentHoldBackQueueSnapshot().isEmpty());
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
