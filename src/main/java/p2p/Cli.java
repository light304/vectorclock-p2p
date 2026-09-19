package p2p;

import java.io.BufferedReader;
import java.util.List;
import java.util.Map;

 // The user-facing command loop for one running peer. Also doubles as a
 // DeliveryManager.DeliveryListener and FailureDetector.SuspicionListener so
 // incoming chat messages and liveness changes get printed as they happen,
 // not just when the user runs a command.
 //
 // The command-dispatch logic (handleCommand) is deliberately separated from
 // the blocking read loop (run) so it can be exercised directly in tests
 // without needing to simulate real terminal input.
final class Cli implements DeliveryManager.DeliveryListener, FailureDetector.SuspicionListener {

    private final String selfId;
    private final DeliveryManager deliveryManager;
    private final FailureDetector failureDetector;
    private final OutboundRouter outboundRouter;
    private final Runnable onCrash;
    private final Runnable onQuit;
    private final BufferedReader stdin;

    Cli(String selfId, DeliveryManager deliveryManager, FailureDetector failureDetector,
        OutboundRouter outboundRouter, BufferedReader stdin, Runnable onCrash, Runnable onQuit) {
        this.selfId = selfId;
        this.deliveryManager = deliveryManager;
        this.failureDetector = failureDetector;
        this.outboundRouter = outboundRouter;
        this.stdin = stdin;
        this.onCrash = onCrash;
        this.onQuit = onQuit;
    }

         // Blocking loop: reads lines from stdin until EOF or a quit/crash
     // command.
     //
     // PeerLog.flush() is called at exactly two points: right before we
     // print the prompt and block on readLine(), and right after readLine()
     // returns. Between those two calls, this thread is either printing our
     // own prompt or waiting on the OS for a line of terminal input - never
     // anything the user is watching mid-keystroke - so background log lines
     // (heartbeats, deliveries, suspicions) can never splice themselves into
     // text the user is currently typing. Anything queued up while they were
     // typing simply appears as a batch the instant they press Enter,
     // ahead of that command's own output.
    void run() {
        printWelcome();
        try {
            String line;
            while (true) {
                PeerLog.flush();
                System.out.print("> ");
                System.out.flush();

                line = stdin.readLine();
                if (line == null) {
                    break;
                }
                PeerLog.flush();

                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                handleCommand(line);
                PeerLog.flush();
            }
        } catch (java.io.IOException e) {
            System.out.println("Input stream closed unexpectedly: " + e.getMessage());
        }
    }

    // Parses and executes exactly one command line. Exposed separately from run() so it's directly testable.
    void handleCommand(String line) {
        String[] parts = line.split("\\s+", 2);
        String command = parts[0].toLowerCase();
        String rest = parts.length > 1 ? parts[1] : "";

        switch (command) {
            case "send" -> handleSend(rest);
            case "clock" -> handleClock();
            case "peers" -> handlePeers();
            case "queue" -> handleQueue();
            case "history" -> handleHistory();
            case "delay" -> handleDelay(rest);
            case "crash" -> handleCrash();
            case "quit", "exit" -> handleQuit();
            case "help" -> printHelp();
            default -> System.out.println("Unknown command: '" + command + "'. Type 'help' for a list of commands.");
        }
    }

    private void handleSend(String rest) {
        if (rest.isEmpty()) {
            System.out.println("Usage: send <text>            (broadcast to everyone)");
            System.out.println("       send @<peerId> <text>   (direct message to one peer)");
            return;
        }
        if (rest.startsWith("@")) {
            String[] targetAndBody = rest.substring(1).split("\\s+", 2);
            if (targetAndBody.length < 2 || targetAndBody[1].isBlank()) {
                System.out.println("Usage: send @<peerId> <text>");
                return;
            }
            String targetId = targetAndBody[0];
            String body = targetAndBody[1];
            try {
                outboundRouter.directChat(targetId, body);
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }
        } else {
            outboundRouter.broadcastChat(rest);
        }
    }

    private void handleClock() {
        System.out.println("Local vector clock (" + selfId + "): " + deliveryManager.currentClockSnapshot());
    }

    private void handlePeers() {
        Map<String, String> status = failureDetector.statusSnapshot();
        if (status.isEmpty()) {
            System.out.println("No other peers are configured.");
            return;
        }
        System.out.println("Peer status:");
        for (Map.Entry<String, String> entry : status.entrySet()) {
            int delay = outboundRouter.getDelay(entry.getKey());
            String delayNote = delay > 0 ? "  [outgoing delay: " + delay + "s]" : "";
            System.out.println("  " + entry.getKey() + ": " + entry.getValue() + delayNote);
        }
    }

    private void handleQueue() {
        List<Message> queue = deliveryManager.currentHoldBackQueueSnapshot();
        if (queue.isEmpty()) {
            System.out.println("Hold-back queue is empty - nothing is waiting to be delivered.");
            return;
        }
        System.out.println("Buffered (not yet causally deliverable) messages:");
        for (Message m : queue) {
            String bodyPart = m.getBody().isEmpty() ? "" : "  \"" + m.getBody() + "\"";
            System.out.println("  " + m.getType() + " from " + m.getSenderId() + "  clock=" + m.getClock() + bodyPart);
        }
    }

    private void handleHistory() {
        List<String> history = deliveryManager.recentActivitySnapshot();
        if (history.isEmpty()) {
            System.out.println("No activity yet.");
            return;
        }
        for (String entry : history) {
            System.out.println("  " + entry);
        }
    }

    private void handleDelay(String rest) {
        String[] delayParts = rest.split("\\s+");
        if (delayParts.length != 2 || delayParts[0].isBlank()) {
            System.out.println("Usage: delay <peerId> <seconds>   (0 clears an existing delay)");
            return;
        }
        String peerId = delayParts[0];
        int seconds;
        try {
            seconds = Integer.parseInt(delayParts[1]);
        } catch (NumberFormatException e) {
            System.out.println("Seconds must be a whole number.");
            return;
        }
        try {
            outboundRouter.setDelay(peerId, seconds);
            if (seconds > 0) {
                System.out.println("Outgoing messages to " + peerId + " will now be delayed by " + seconds + "s.");
            } else {
                System.out.println("Delay to " + peerId + " cleared.");
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }

    private void handleCrash() {
        System.out.println("Simulating a crash: no goodbye message, no graceful shutdown. Exiting now.");
        onCrash.run();
    }

    private void handleQuit() {
        System.out.println("Shutting down " + selfId + " intentionally...");
        onQuit.run();
    }

    private void printWelcome() {
        System.out.println("=================================================");
        System.out.println(" Peer '" + selfId + "' is running. Type 'help' for commands.");
        System.out.println("=================================================");
    }

    private void printHelp() {
        System.out.println("Commands:");
        System.out.println("  send <text>              broadcast a chat message to every peer");
        System.out.println("  send @<peerId> <text>    send a chat message to one peer only");
        System.out.println("  clock                    show this peer's current vector clock");
        System.out.println("  peers                    show known peers and their alive/suspected status");
        System.out.println("  queue                    show messages buffered pending causal delivery");
        System.out.println("  history                  show recent send/deliver activity");
        System.out.println("  delay <peerId> <secs>    artificially delay outgoing messages to one peer (0 clears)");
        System.out.println("  crash                    simulate this peer failing (no goodbye)");
        System.out.println("  quit / exit              shut this peer down intentionally");
        System.out.println("  help                     show this list again");
    }

    // ---------- DeliveryManager.DeliveryListener ----------

    @Override
    public void onDelivered(Message message) {
        // Queued, not printed directly: this callback fires from whatever
        // thread delivered the message (usually a Server connection-handler
        // thread), not the CLI thread - see PeerLog's javadoc for why.
        if (message.getType() == MessageType.JOIN) {
            PeerLog.queue(">> " + message.getSenderId() + " has joined.");
        } else if (message.getType() == MessageType.CHAT) {
            String label = message.isBroadcast() ? message.getSenderId() : message.getSenderId() + " -> you (direct)";
            PeerLog.queue(label + ": " + message.getBody());
        }
    }

    // ---------- FailureDetector.SuspicionListener ----------

    @Override
    public void onSuspected(String peerId) {
        // Queued: fires from FailureDetector's own timer thread.
        PeerLog.queue(">> " + peerId + " appears to be unreachable (no contact recently).");
    }

    @Override
    public void onRecovered(String peerId) {
        // Queued: fires from FailureDetector's own timer thread.
        PeerLog.queue(">> " + peerId + " is responding again.");
    }
}
