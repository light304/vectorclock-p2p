package p2p;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;

 // Standalone harness for FailureDetector. Uses a manually-advanced fake
 // clock (a simple mutable long holder) instead of real Thread.sleep() calls,
 // so the timeout logic can be tested precisely and without flakiness.
 // Run with: java p2p.FailureDetectorTest
public class FailureDetectorTest {

    private static int passed = 0;
    private static int failed = 0;

    // A trivial mutable clock we control by hand from the test.
    private static class FakeClock {
        long millis = 0;
        long nowMillis() { return millis; }
        void advanceBy(long millisToAdd) { millis += millisToAdd; }
    }

    public static void main(String[] args) {
        testNeverContactedPeerIsUnknownNotSuspected();
        testRecentContactMeansAlive();
        testGoesSilentPastTimeoutBecomesSuspected();
        testRecoveryClearsSuspicion();
        testListenerIsNotifiedOnSuspicionAndRecovery();
        testRepeatedChecksDoNotDoubleNotify();

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

    private static void testNeverContactedPeerIsUnknownNotSuspected() {
        FakeClock clock = new FakeClock();
        FailureDetector detector = new FailureDetector(peers("peer-B", "peer-C"), 5000, 1000, clock::nowMillis);

        clock.advanceBy(999_999); // a huge amount of time passes with zero contact ever
        detector.checkForTimeouts();

        check("never-contacted peer stays UNKNOWN, never SUSPECTED",
                detector.statusSnapshot().get("peer-B").startsWith("UNKNOWN")
                        && detector.statusSnapshot().get("peer-C").startsWith("UNKNOWN"));
    }

    private static void testRecentContactMeansAlive() {
        FakeClock clock = new FakeClock();
        FailureDetector detector = new FailureDetector(peers("peer-B"), 5000, 1000, clock::nowMillis);

        detector.recordSeen("peer-B");
        clock.advanceBy(1000); // well within the 5000ms timeout
        detector.checkForTimeouts();

        check("recent contact reports ALIVE", detector.statusSnapshot().get("peer-B").startsWith("ALIVE"));
    }

    private static void testGoesSilentPastTimeoutBecomesSuspected() {
        FakeClock clock = new FakeClock();
        FailureDetector detector = new FailureDetector(peers("peer-B"), 5000, 1000, clock::nowMillis);

        detector.recordSeen("peer-B");
        clock.advanceBy(5001); // just past the timeout
        detector.checkForTimeouts();

        check("silence past the timeout becomes SUSPECTED",
                detector.statusSnapshot().get("peer-B").equals("SUSPECTED"));
    }

    private static void testRecoveryClearsSuspicion() {
        FakeClock clock = new FakeClock();
        FailureDetector detector = new FailureDetector(peers("peer-B"), 5000, 1000, clock::nowMillis);

        detector.recordSeen("peer-B");
        clock.advanceBy(6000);
        detector.checkForTimeouts();
        check("setup: peer-B is SUSPECTED before recovery", detector.statusSnapshot().get("peer-B").equals("SUSPECTED"));

        detector.recordSeen("peer-B"); // peer-B contacts us again
        check("recovery: peer-B is ALIVE again after being SUSPECTED",
                detector.statusSnapshot().get("peer-B").startsWith("ALIVE"));
    }

    private static void testListenerIsNotifiedOnSuspicionAndRecovery() {
        FakeClock clock = new FakeClock();
        FailureDetector detector = new FailureDetector(peers("peer-B"), 5000, 1000, clock::nowMillis);

        List<String> suspectedEvents = new ArrayList<>();
        List<String> recoveredEvents = new ArrayList<>();
        detector.addListener(new FailureDetector.SuspicionListener() {
            @Override public void onSuspected(String peerId) { suspectedEvents.add(peerId); }
            @Override public void onRecovered(String peerId) { recoveredEvents.add(peerId); }
        });

        detector.recordSeen("peer-B");
        clock.advanceBy(6000);
        detector.checkForTimeouts();
        check("listener notified exactly once on suspicion",
                suspectedEvents.size() == 1 && suspectedEvents.get(0).equals("peer-B"));

        detector.recordSeen("peer-B");
        check("listener notified exactly once on recovery",
                recoveredEvents.size() == 1 && recoveredEvents.get(0).equals("peer-B"));
    }

    private static void testRepeatedChecksDoNotDoubleNotify() {
        FakeClock clock = new FakeClock();
        FailureDetector detector = new FailureDetector(peers("peer-B"), 5000, 1000, clock::nowMillis);

        List<String> suspectedEvents = new ArrayList<>();
        detector.addListener(new FailureDetector.SuspicionListener() {
            @Override public void onSuspected(String peerId) { suspectedEvents.add(peerId); }
            @Override public void onRecovered(String peerId) { }
        });

        detector.recordSeen("peer-B");
        clock.advanceBy(6000);
        detector.checkForTimeouts();
        detector.checkForTimeouts(); // called again while still silent - should NOT re-notify
        detector.checkForTimeouts();

        check("repeated checks while still silent only notify once",
                suspectedEvents.size() == 1);
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
