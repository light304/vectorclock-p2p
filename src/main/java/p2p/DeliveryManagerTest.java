package p2p;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

 // Standalone harness for DeliveryManager. Simulates message exchange between
 // several in-memory DeliveryManager instances (no sockets involved) so the
 // causal delivery / hold-back-queue logic can be verified in isolation,
 // exactly the way VectorClockTest and MessageTest verified their layers.
 // Run with: java p2p.DeliveryManagerTest
public class DeliveryManagerTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        testInOrderDeliveryNeedsNoBuffering();
        testOutOfOrderMessageIsBufferedThenDelivered();
        testHeartbeatIsLivenessOnlyAndDoesNotTouchClock();
        testDirectMessageDeliveredNormally();
        testTheActualDemoScenario_causalChainAcrossThreePeers();

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

        // Deliver out of order: second arrives before first.
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

    private static void testHeartbeatIsLivenessOnlyAndDoesNotTouchClock() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        List<Message> delivered = new ArrayList<>();
        b.addListener(delivered::add);

        a.prepareForSend(MessageType.CHAT, null, "bump A's clock to 1"); // not sent to b in this test
        Message heartbeat = a.buildHeartbeat();

        b.onMessageReceived(heartbeat);

        check("heartbeat: does NOT merge into the recipient's clock (liveness-only, see DeliveryManager javadoc)",
                b.currentClockSnapshot().get("peer-A") == 0);
        check("heartbeat: does NOT trigger a DeliveryListener callback", delivered.isEmpty());
        check("heartbeat: never enters the hold-back queue", b.currentHoldBackQueueSnapshot().isEmpty());
    }

    private static void testDirectMessageDeliveredNormally() {
        Set<String> allPeers = peers("peer-A", "peer-B", "peer-C");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        List<Message> delivered = new ArrayList<>();
        b.addListener(delivered::add);

        Message dm = a.prepareForSend(MessageType.CHAT, "peer-B", "just for you");
        b.onMessageReceived(dm);

        check("direct message: delivered normally like a broadcast", delivered.size() == 1);
        check("direct message: body intact", delivered.get(0).getBody().equals("just for you"));
    }

         // This mirrors the exact scenario planned for the live demo:
     // - A broadcasts "question" to B and C.
     // - B delivers it, then broadcasts "answer" to A and C.
     // - C receives "answer" BEFORE "question" (simulated network delay).
     // Expected: C buffers "answer" until "question" arrives, then delivers
     // both, in causal order. This is also a regression test for the
     // increment-on-deliver bug caught while designing these tests: if
     // delivering a message incremented the local clock, "answer" would never
     // become deliverable at all.
    private static void testTheActualDemoScenario_causalChainAcrossThreePeers() {
        Set<String> allPeers = peers("peer-A", "peer-B", "peer-C");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);
        DeliveryManager c = new DeliveryManager("peer-C", allPeers);

        List<Message> deliveredAtC = new ArrayList<>();
        c.addListener(deliveredAtC::add);

        Message question = a.prepareForSend(MessageType.CHAT, null, "question");
        b.onMessageReceived(question); // B delivers it immediately (in order)

        Message answer = b.prepareForSend(MessageType.CHAT, null, "answer");

        // Simulate network delay: C sees "answer" before "question".
        c.onMessageReceived(answer);
        check("demo scenario: 'answer' arrives first and is buffered at C",
                c.currentHoldBackQueueSnapshot().size() == 1);
        check("demo scenario: nothing delivered to C yet", deliveredAtC.isEmpty());

        c.onMessageReceived(question);
        check("demo scenario: once 'question' arrives, BOTH messages deliver (queue drains)",
                c.currentHoldBackQueueSnapshot().isEmpty());
        check("demo scenario: C delivered exactly 2 messages", deliveredAtC.size() == 2);
        check("demo scenario: delivery order is causally correct (question, then answer)",
                deliveredAtC.get(0).getBody().equals("question")
                        && deliveredAtC.get(1).getBody().equals("answer"));

        check("demo scenario: C's final clock reflects both A's and B's events",
                c.currentClockSnapshot().get("peer-A") == 1
                        && c.currentClockSnapshot().get("peer-B") == 1);
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
