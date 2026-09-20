package p2p;

import java.util.Arrays;
import java.util.List;

 // Standalone harness to prove VectorClock is correct before anything else in
 // the project depends on it. Run with:
 // java p2p.VectorClockTest
 // No JUnit / Maven required - deliberately plain so it can be run anywhere
 // with just a JDK, including inside a fresh clone before build tooling is set up.
public class VectorClockTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        List<String> peers = Arrays.asList("peer-A", "peer-B", "peer-C");

        testFreshClockStartsAtZero(peers);
        testIncrementOnlyAffectsOwner(peers);
        testMergeTakesEntrywiseMax(peers);
        testCompareEqual(peers);
        testCompareBeforeAndAfter(peers);
        testCompareConcurrent(peers);
        testSnapshotIsIndependentCopy(peers);
        testRealisticScenario_causalChain(peers);

        System.out.println();
        System.out.println("---------------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("---------------------------------------------");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void testFreshClockStartsAtZero(List<String> peers) {
        VectorClock clock = new VectorClock(peers);
        check("fresh clock: peer-A starts at 0", clock.get("peer-A") == 0);
        check("fresh clock: unknown peer defaults to 0", clock.get("peer-Z") == 0);
    }

    private static void testIncrementOnlyAffectsOwner(List<String> peers) {
        VectorClock clock = new VectorClock(peers);
        clock.increment("peer-A");
        clock.increment("peer-A");

        check("increment: peer-A is now 2", clock.get("peer-A") == 2);
        check("increment: peer-B untouched", clock.get("peer-B") == 0);
        check("increment: peer-C untouched", clock.get("peer-C") == 0);
    }

    private static void testMergeTakesEntrywiseMax(List<String> peers) {
        VectorClock local = new VectorClock(peers);
        local.increment("peer-A"); // local = {A:1, B:0, C:0}

        VectorClock incoming = new VectorClock(peers);
        incoming.increment("peer-B");
        incoming.increment("peer-B");
        incoming.increment("peer-C"); // incoming = {A:0, B:2, C:1}

        local.mergeWith(incoming); // expect {A:1, B:2, C:1}

        check("merge: keeps local's higher A", local.get("peer-A") == 1);
        check("merge: takes incoming's higher B", local.get("peer-B") == 2);
        check("merge: takes incoming's higher C", local.get("peer-C") == 1);
    }

    private static void testCompareEqual(List<String> peers) {
        VectorClock a = new VectorClock(peers);
        VectorClock b = new VectorClock(peers);
        a.increment("peer-A");
        b.increment("peer-A");

        check("compare: identical clocks are EQUAL",
                a.compareTo(b) == VectorClock.Relation.EQUAL);
    }

    private static void testCompareBeforeAndAfter(List<String> peers) {
        VectorClock earlier = new VectorClock(peers);
        earlier.increment("peer-A"); // {A:1, B:0, C:0}

        VectorClock later = new VectorClock(peers);
        later.mergeWith(earlier);
        later.increment("peer-A"); // {A:2, B:0, C:0}

        check("compare: earlier is BEFORE later",
                earlier.compareTo(later) == VectorClock.Relation.BEFORE);
        check("compare: later is AFTER earlier",
                later.compareTo(earlier) == VectorClock.Relation.AFTER);
    }

    private static void testCompareConcurrent(List<String> peers) {
        // Two peers act independently without seeing each other's update first.
        VectorClock fromA = new VectorClock(peers);
        fromA.increment("peer-A"); // {A:1, B:0, C:0}

        VectorClock fromB = new VectorClock(peers);
        fromB.increment("peer-B"); // {A:0, B:1, C:0}

        check("compare: independent updates are CONCURRENT",
                fromA.compareTo(fromB) == VectorClock.Relation.CONCURRENT);
        check("compare: concurrency is symmetric",
                fromB.compareTo(fromA) == VectorClock.Relation.CONCURRENT);
    }

    private static void testSnapshotIsIndependentCopy(List<String> peers) {
        VectorClock clock = new VectorClock(peers);
        clock.increment("peer-A");

        var snapshot = clock.snapshot();
        clock.increment("peer-A"); // mutate the live clock after taking the snapshot

        check("snapshot: does not change after live clock mutates",
                snapshot.get("peer-A") == 1);
        check("snapshot: live clock did advance",
                clock.get("peer-A") == 2);

        boolean threwOnMutationAttempt = false;
        try {
            snapshot.put("peer-A", 99);
        } catch (UnsupportedOperationException expected) {
            threwOnMutationAttempt = true;
        }
        check("snapshot: is unmodifiable", threwOnMutationAttempt);
    }

         // Mirrors the exact scenario planned for the live demo: A sends to B and C,
     // B replies after seeing A's message, and we confirm the causal chain is
     // captured correctly in the raw clock values (delivery-buffering logic
     // itself lives in DeliveryManager and is tested separately).
    private static void testRealisticScenario_causalChain(List<String> peers) {
        VectorClock clockA = new VectorClock(peers);
        VectorClock clockB = new VectorClock(peers);
        VectorClock clockC = new VectorClock(peers);

        // A sends "question" to B and C.
        clockA.increment("peer-A"); // A: {A:1,B:0,C:0}
        var questionClock = VectorClock.fromSnapshot(clockA.snapshot());

        // B receives and delivers "question": merge then increment.
        clockB.mergeWith(questionClock).increment("peer-B"); // B: {A:1,B:1,C:0}

        // B sends "answer" to A and C, carrying its own clock at this point.
        var answerClock = VectorClock.fromSnapshot(clockB.snapshot());

        // C receives "question" first (as expected) and delivers it.
        clockC.mergeWith(questionClock).increment("peer-C"); // C: {A:1,B:0,C:1}

        // C then receives "answer" and delivers it.
        clockC.mergeWith(answerClock).increment("peer-C"); // C: {A:1,B:1,C:2}

        check("scenario: C causally follows both A's question and B's answer",
                questionClock.compareTo(clockC) == VectorClock.Relation.BEFORE
                        && answerClock.compareTo(clockC) == VectorClock.Relation.BEFORE);

        check("scenario: A's question happened-before B's answer",
                questionClock.compareTo(answerClock) == VectorClock.Relation.BEFORE);
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
