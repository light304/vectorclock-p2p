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
 
 // Covers both the original causal-delivery scenarios and the per-channel
 // ("pairwise clock") scenarios: every sender-to-receiver channel gets its
 // own sequence number, so a receiver's FIFO check only ever waits on
 // messages actually addressed to it - a direct message to someone else
 // entirely can no longer block delivery on an unrelated channel.
 
 // Also covers the reconnect handshake (JOIN / SYNC, see
 // DeliveryManager.buildControl() / adoptClock()): a rejoining peer's clock
 // and per-channel counters reset to zero on restart, and SYNC replies from
 // still-running peers are what let it resume exactly where it left off on
 // every channel, instead of either wedging forever or restarting numbering
 // from 1 and looking like a stream of stale duplicates to everyone else.
 
 public class DeliveryManagerTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        testInOrderDeliveryNeedsNoBuffering();
        testOutOfOrderMessageIsBufferedThenDelivered();
        testHeartbeatIsLivenessOnlyAndDoesNotTouchClock();
        testDirectMessageDeliveredNormally();
        testTheActualDemoScenario_causalChainAcrossThreePeers();
        testBroadcastDirectToOtherPeerBroadcast_directDoesNotBlockChannel();
        testDelayedDirectMessageOvertakenByLaterBroadcastOnSameChannel();
        testTwoDirectMessagesArriveSwapped();
        testDirectMessageAfterBroadcastOvertakesThatBroadcast();
        testLostDirectMessageLeavesReceiverWaitingForever();
        testJoinAndSyncDoNotTickClockOrConsumeChannelSeq();
        testRestartedPeerResumesChannelNumberingAfterSync();
        testCatchUpDiscardsQueuedMessageCoveredByChannelRestore();
        testAdoptingAStaleSyncNeverMovesChannelCountersBackward();

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

    // Convenience: broadcasts to exactly the given receivers and returns the
    // one Message meant for a single named receiver, for tests that only
    // care about tracking one recipient's copy.
    private static Message broadcastTo(DeliveryManager sender, String body, String... receiverIds) {
        return sender.prepareForBroadcast(MessageType.CHAT, body, Arrays.asList(receiverIds)).get(0);
    }

    private static void testInOrderDeliveryNeedsNoBuffering() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        Message m1 = broadcastTo(a, "hello", "peer-B");
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

        Message first = broadcastTo(a, "first", "peer-B");
        Message second = broadcastTo(a, "second", "peer-B");

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

        a.prepareForSend(MessageType.CHAT, "peer-B", "bump A's clock to 1"); // not sent to b in this test
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

        List<Message> questionCopies = a.prepareForBroadcast(MessageType.CHAT, "question", List.of("peer-B", "peer-C"));
        b.onMessageReceived(questionCopies.get(0)); // B delivers it immediately (in order)

        List<Message> answerCopies = b.prepareForBroadcast(MessageType.CHAT, "answer", List.of("peer-A", "peer-C"));
        Message answerForC = answerCopies.get(1);
        Message questionForC = questionCopies.get(1);

        // Simulate network delay: C sees "answer" before "question".
        c.onMessageReceived(answerForC);
        check("demo scenario: 'answer' arrives first and is buffered at C",
                c.currentHoldBackQueueSnapshot().size() == 1);
        check("demo scenario: nothing delivered to C yet", deliveredAtC.isEmpty());

        c.onMessageReceived(questionForC);
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

    // The bug this feature exists to fix: A broadcasts to B and C, then
    // sends a DIRECT message to C ONLY, then broadcasts again. Under the old
    // single-shared-counter-per-sender scheme, B would expect A's messages
    // in one global sequence and get stuck forever waiting for the
    // C-only message it was never going to receive. Under per-channel
    // counters, B's channel from A only counts messages actually sent to B,
    // so it never even notices the C-only message exists.
    private static void testBroadcastDirectToOtherPeerBroadcast_directDoesNotBlockChannel() {
        Set<String> allPeers = peers("peer-A", "peer-B", "peer-C");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);
        DeliveryManager c = new DeliveryManager("peer-C", allPeers);

        List<Message> deliveredAtB = new ArrayList<>();
        b.addListener(deliveredAtB::add);

        List<Message> broadcast1 = a.prepareForBroadcast(MessageType.CHAT, "one", List.of("peer-B", "peer-C"));
        b.onMessageReceived(broadcast1.get(0));
        c.onMessageReceived(broadcast1.get(1));

        Message directToC = a.prepareForSend(MessageType.CHAT, "peer-C", "only for C");
        c.onMessageReceived(directToC);

        List<Message> broadcast3 = a.prepareForBroadcast(MessageType.CHAT, "three", List.of("peer-B", "peer-C"));
        b.onMessageReceived(broadcast3.get(0));

        check("channel isolation: B delivered both broadcasts despite never seeing the C-only direct",
                deliveredAtB.size() == 2);
        check("channel isolation: B's copy of the second broadcast is nothing stuck in its queue",
                b.currentHoldBackQueueSnapshot().isEmpty());
        check("channel isolation: delivery order at B is correct ('one' then 'three')",
                deliveredAtB.get(0).getBody().equals("one") && deliveredAtB.get(1).getBody().equals("three"));
    }

    private static void testDelayedDirectMessageOvertakenByLaterBroadcastOnSameChannel() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        List<Message> deliveredAtB = new ArrayList<>();
        b.addListener(deliveredAtB::add);

        // A direct message to B, and a later broadcast that also reaches B,
        // both live on the same A -> B channel and must be delivered in the
        // order they were sent, however they actually arrive.
        Message directMsg = a.prepareForSend(MessageType.CHAT, "peer-B", "delayed direct");
        Message broadcastMsg = broadcastTo(a, "later broadcast", "peer-B");

        b.onMessageReceived(broadcastMsg); // arrives first, out of order
        check("delayed direct: later broadcast is buffered, not delivered early",
                b.currentHoldBackQueueSnapshot().size() == 1);

        b.onMessageReceived(directMsg); // the delayed one finally arrives
        check("delayed direct: both deliver once the direct message catches up",
                deliveredAtB.size() == 2);
        check("delayed direct: correct causal order (direct, then broadcast)",
                deliveredAtB.get(0).getBody().equals("delayed direct")
                        && deliveredAtB.get(1).getBody().equals("later broadcast"));
    }

    private static void testTwoDirectMessagesArriveSwapped() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        List<Message> deliveredAtB = new ArrayList<>();
        b.addListener(deliveredAtB::add);

        Message first = a.prepareForSend(MessageType.CHAT, "peer-B", "first direct");
        Message second = a.prepareForSend(MessageType.CHAT, "peer-B", "second direct");

        b.onMessageReceived(second); // arrives first
        check("swapped directs: second is buffered until first arrives",
                b.currentHoldBackQueueSnapshot().size() == 1);

        b.onMessageReceived(first);
        check("swapped directs: both deliver in the correct order",
                deliveredAtB.size() == 2
                        && deliveredAtB.get(0).getBody().equals("first direct")
                        && deliveredAtB.get(1).getBody().equals("second direct"));
    }

    private static void testDirectMessageAfterBroadcastOvertakesThatBroadcast() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        List<Message> deliveredAtB = new ArrayList<>();
        b.addListener(deliveredAtB::add);

        Message broadcastMsg = broadcastTo(a, "broadcast first", "peer-B");
        Message directMsg = a.prepareForSend(MessageType.CHAT, "peer-B", "direct second");

        b.onMessageReceived(directMsg); // the later message overtakes the broadcast in transit
        check("direct overtakes broadcast: held back until the broadcast arrives",
                b.currentHoldBackQueueSnapshot().size() == 1);
        check("direct overtakes broadcast: nothing delivered yet", deliveredAtB.isEmpty());

        b.onMessageReceived(broadcastMsg);
        check("direct overtakes broadcast: both deliver, in the order they were sent",
                deliveredAtB.size() == 2
                        && deliveredAtB.get(0).getBody().equals("broadcast first")
                        && deliveredAtB.get(1).getBody().equals("direct second"));
    }

    // A direct message that never arrives (lost, not just delayed) leaves
    // the receiver correctly waiting rather than delivering out of order or
    // getting stuck in some inconsistent state. There is no retransmission
    // in this system, so this is the expected - if unfortunate - outcome.
    private static void testLostDirectMessageLeavesReceiverWaitingForever() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        List<Message> deliveredAtB = new ArrayList<>();
        b.addListener(deliveredAtB::add);

        a.prepareForSend(MessageType.CHAT, "peer-B", "this one gets lost"); // never delivered to b
        Message second = a.prepareForSend(MessageType.CHAT, "peer-B", "this one arrives");

        b.onMessageReceived(second);
        check("lost direct: the arriving message is buffered, not delivered",
                b.currentHoldBackQueueSnapshot().size() == 1);
        check("lost direct: nothing delivered while waiting on the lost message",
                deliveredAtB.isEmpty());
    }

    // ---------------- reconnect: JOIN / SYNC catch-up ----------------

    private static void testJoinAndSyncDoNotTickClockOrConsumeChannelSeq() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        Message join = b.buildControl(MessageType.JOIN, null);
        check("join: building it does not tick the sender's own clock",
                b.currentClockSnapshot().get("peer-B") == 0);

        List<Message> notified = new ArrayList<>();
        a.addListener(notified::add);
        a.onMessageReceived(join);

        check("join: never enters the hold-back queue", a.currentHoldBackQueueSnapshot().isEmpty());
        check("join: listeners are told about it immediately", notified.size() == 1);
        check("join: receiver's clock is untouched (joiner's clock is stale, so it is not merged)",
                a.currentClockSnapshot().get("peer-B") == 0);
    }

    // The exact reconnect scenario: B sends a couple of broadcasts, crashes,
    // and restarts with a brand-new (all-zero) DeliveryManager. Without the
    // JOIN/SYNC handshake, the B->A and B->C channels would restart
    // numbering from 1 and look like stale duplicates, while the A->B and
    // C->B channels would still expect B to be picking up from wherever it
    // left off and could stay buffered forever.
    private static void testRestartedPeerResumesChannelNumberingAfterSync() {
        Set<String> allPeers = peers("peer-A", "peer-B", "peer-C");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);
        DeliveryManager c = new DeliveryManager("peer-C", allPeers);

        // Normal life before the crash: B broadcasts twice, A broadcasts once.
        for (String body : new String[] {"b one", "b two"}) {
            List<Message> copies = b.prepareForBroadcast(MessageType.CHAT, body, List.of("peer-A", "peer-C"));
            a.onMessageReceived(copies.get(0));
            c.onMessageReceived(copies.get(1));
        }
        List<Message> aCopies = a.prepareForBroadcast(MessageType.CHAT, "a one", List.of("peer-B", "peer-C"));
        b.onMessageReceived(aCopies.get(0));
        c.onMessageReceived(aCopies.get(1));

        // B crashes: a fresh DeliveryManager, clock and channel counters all zero.
        DeliveryManager reborn = new DeliveryManager("peer-B", allPeers);
        check("restart: a fresh peer really does start at zero", reborn.currentClockSnapshot().get("peer-B") == 0);

        // B announces itself; A and C answer with a SYNC; B adopts both.
        Message join = reborn.buildControl(MessageType.JOIN, null);
        a.onMessageReceived(join);
        c.onMessageReceived(join);
        reborn.onMessageReceived(a.buildControl(MessageType.SYNC, "peer-B"));
        reborn.onMessageReceived(c.buildControl(MessageType.SYNC, "peer-B"));

        check("catch-up: own clock entry restored to the highest value anyone saw from it",
                reborn.currentClockSnapshot().get("peer-B") == 2);

        // The reborn peer's next message to A must resume at channelSeq 3
        // (A already delivered B's first two) and deliver immediately.
        Message next = reborn.prepareForSend(MessageType.CHAT, "peer-A", "b three");
        check("catch-up: outgoing channel to A resumes at 3, not 1", next.getChannelSeq() == 3);
        a.onMessageReceived(next);
        check("catch-up: A delivers it immediately (channel counters agree)",
                a.currentHoldBackQueueSnapshot().isEmpty());

        // And the reverse direction: A's next message reaches the reborn
        // peer without getting stuck waiting on the pre-crash sequence.
        Message a2 = a.prepareForSend(MessageType.CHAT, "peer-B", "a two");
        reborn.onMessageReceived(a2);
        check("catch-up: reborn peer delivers new messages from A immediately (not waiting on the old channel position)",
                reborn.currentHoldBackQueueSnapshot().isEmpty());
    }

    // A message already sitting in the hold-back queue can be made
    // permanently un-deliverable by a SYNC (its channel position is now in
    // the past). It must be discarded, not left in the queue forever - this
    // is the documented "catching up means skipping history" trade-off.
    private static void testCatchUpDiscardsQueuedMessageCoveredByChannelRestore() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        a.prepareForSend(MessageType.CHAT, "peer-B", "missed");        // channelSeq 1, never reaches B
        Message second = a.prepareForSend(MessageType.CHAT, "peer-B", "arrives alone"); // channelSeq 2
        b.onMessageReceived(second);
        check("discard: message with a gap ahead of it is buffered first",
                b.currentHoldBackQueueSnapshot().size() == 1);

        b.onMessageReceived(a.buildControl(MessageType.SYNC, "peer-B")); // A reports its channel to B is at 2
        check("discard: the now-unreachable buffered message is removed from the queue",
                b.currentHoldBackQueueSnapshot().isEmpty());

        Message third = a.prepareForSend(MessageType.CHAT, "peer-B", "three");
        check("discard: channel position is restored - the next real message (3) delivers immediately",
                third.getChannelSeq() == 3);
        b.onMessageReceived(third);
        check("discard: ...and it actually does deliver, not buffer again",
                b.currentHoldBackQueueSnapshot().isEmpty());
    }

    private static void testAdoptingAStaleSyncNeverMovesChannelCountersBackward() {
        Set<String> allPeers = peers("peer-A", "peer-B");
        DeliveryManager a = new DeliveryManager("peer-A", allPeers);
        DeliveryManager b = new DeliveryManager("peer-B", allPeers);

        b.prepareForSend(MessageType.CHAT, "peer-A", "x");
        b.prepareForSend(MessageType.CHAT, "peer-A", "y"); // B's outgoing channel to A is now at 2

        // A stale/duplicate SYNC from a peer that has seen nothing yet.
        b.onMessageReceived(a.buildControl(MessageType.SYNC, "peer-B"));

        Message next = b.prepareForSend(MessageType.CHAT, "peer-A", "z");
        check("monotonic: a behind-the-times sync never rewinds our outgoing channel counter",
                next.getChannelSeq() == 3);
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