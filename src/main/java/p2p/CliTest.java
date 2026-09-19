package p2p;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

 // Standalone harness for Cli. Redirects System.out to capture what the CLI
 // prints for a given command, using real DeliveryManager/FailureDetector/
 // OutboundRouter instances (backed by a real ephemeral-port Server) rather
 // than mocks, consistent with the rest of this project's testing approach.
 // Run with: java p2p.CliTest
public class CliTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        testUnknownCommandShowsHelp();
        testHelpListsCommands();
        testClockCommandShowsSnapshot();
        testEmptyQueueMessage();
        testDelayValidation();
        testDelayOnUnknownPeerIsRejected();
        testSendWithNoArgsShowsUsage();
        testDeliveredChatIsPrinted();
        testDeliveredJoinIsPrinted();
        testSuspicionAndRecoveryArePrinted();
        testCrashInvokesCallbackExactlyOnce();
        testQuitInvokesCallbackExactlyOnce();

        System.out.println();
        System.out.println("---------------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("---------------------------------------------");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static Set<String> peerIdSet(String... ids) {
        return new LinkedHashSet<>(java.util.Arrays.asList(ids));
    }

    // Builds a Cli wired to real (but test-scale) collaborators, with no-op crash/quit unless overridden.
    private static Cli buildCli(String selfId, Set<String> allPeerIds, List<PeerInfo> others,
                                 Runnable onCrash, Runnable onQuit) {
        DeliveryManager dm = new DeliveryManager(selfId, allPeerIds);
        FailureDetector fd = new FailureDetector(peerIdWithoutSelf(allPeerIds, selfId), 5000, 1000);
        OutboundRouter router = new OutboundRouter(dm, others);
        return new Cli(selfId, dm, fd, router, null, onCrash, onQuit);
    }

    private static Set<String> peerIdWithoutSelf(Set<String> all, String selfId) {
        Set<String> others = new LinkedHashSet<>(all);
        others.remove(selfId);
        return others;
    }

    // Runs a block while System.out is captured, and returns everything printed.
    private static String captureOutput(Runnable action) {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buffer));
        try {
            action.run();
        } finally {
            System.setOut(original);
        }
        return buffer.toString();
    }

    private static void testUnknownCommandShowsHelp() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        String output = captureOutput(() -> cli.handleCommand("frobnicate"));
        check("unknown command: points user to 'help'", output.contains("help"));
    }

    private static void testHelpListsCommands() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        String output = captureOutput(() -> cli.handleCommand("help"));
        check("help: mentions send", output.contains("send"));
        check("help: mentions clock", output.contains("clock"));
        check("help: mentions delay", output.contains("delay"));
        check("help: mentions crash", output.contains("crash"));
    }

    private static void testClockCommandShowsSnapshot() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        String output = captureOutput(() -> cli.handleCommand("clock"));
        check("clock: mentions the peer's own id", output.contains("peer-A"));
        check("clock: shows a clock-like structure", output.contains("peer-A=0") || output.contains("peer-A:0")
                || output.contains("{peer-A"));
    }

    private static void testEmptyQueueMessage() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        String output = captureOutput(() -> cli.handleCommand("queue"));
        check("queue: reports empty when nothing is buffered", output.toLowerCase().contains("empty"));
    }

    private static void testDelayValidation() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});

        String badUsage = captureOutput(() -> cli.handleCommand("delay"));
        check("delay: missing args shows usage", badUsage.toLowerCase().contains("usage"));

        String badNumber = captureOutput(() -> cli.handleCommand("delay peer-B notanumber"));
        check("delay: non-numeric seconds is rejected clearly", badNumber.toLowerCase().contains("whole number"));

        String goodDelay = captureOutput(() -> cli.handleCommand("delay peer-B 3"));
        check("delay: valid command confirms the delay", goodDelay.contains("3") && goodDelay.contains("peer-B"));
    }

    private static void testDelayOnUnknownPeerIsRejected() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        String output = captureOutput(() -> cli.handleCommand("delay peer-Z 3"));
        check("delay: unknown peer id is rejected", output.contains("Unknown peer"));
    }

    private static void testSendWithNoArgsShowsUsage() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        String output = captureOutput(() -> cli.handleCommand("send"));
        check("send: no args shows usage for both broadcast and direct forms",
                output.toLowerCase().contains("usage") && output.contains("@"));
    }

    private static void testDeliveredChatIsPrinted() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        Message chat = new Message(MessageType.CHAT, "peer-B", null, "hello there",
                java.util.Map.of("peer-A", 0, "peer-B", 1), System.currentTimeMillis());

        // onDelivered() now queues instead of printing directly (see PeerLog),
        // so the test has to flush before the capture ends to see the line.
        String output = captureOutput(() -> {
            cli.onDelivered(chat);
            PeerLog.flush();
        });
        check("delivered chat: shows sender and body", output.contains("peer-B") && output.contains("hello there"));
    }

    private static void testDeliveredJoinIsPrinted() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});
        Message join = new Message(MessageType.JOIN, "peer-B", null, "",
                java.util.Map.of("peer-A", 0, "peer-B", 1), System.currentTimeMillis());

        String output = captureOutput(() -> {
            cli.onDelivered(join);
            PeerLog.flush();
        });
        check("delivered join: mentions the peer joined", output.contains("peer-B") && output.toLowerCase().contains("joined"));
    }

    private static void testSuspicionAndRecoveryArePrinted() {
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> {});

        String suspectOutput = captureOutput(() -> {
            cli.onSuspected("peer-B");
            PeerLog.flush();
        });
        check("suspicion: mentions the peer", suspectOutput.contains("peer-B"));

        String recoverOutput = captureOutput(() -> {
            cli.onRecovered("peer-B");
            PeerLog.flush();
        });
        check("recovery: mentions the peer", recoverOutput.contains("peer-B"));
    }

    private static void testCrashInvokesCallbackExactlyOnce() {
        AtomicBoolean crashed = new AtomicBoolean(false);
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> crashed.set(true), () -> {});
        captureOutput(() -> cli.handleCommand("crash"));
        check("crash: invokes the onCrash callback", crashed.get());
    }

    private static void testQuitInvokesCallbackExactlyOnce() {
        AtomicBoolean quit = new AtomicBoolean(false);
        Cli cli = buildCli("peer-A", peerIdSet("peer-A", "peer-B"), List.of(new PeerInfo("peer-B", "localhost", 1)),
                () -> {}, () -> quit.set(true));
        captureOutput(() -> cli.handleCommand("quit"));
        check("quit: invokes the onQuit callback", quit.get());
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
