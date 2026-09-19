package p2p;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;

 // Tiny shared logger so every part of the peer (delivery, networking,
 // failure detection) prints in the same format. This is not for debugging -
 // these log lines ARE the evidence of the vector clock mechanism working,
 // so the format is deliberately readable rather than terse.
 //
 // Format: [HH:mm:ss] CATEGORY   message
 //
 // IMPORTANT - this class never writes to System.out itself. Almost every
 // caller here runs on a background thread (the socket accept loop, the
 // outbound-sender pool, the heartbeat/failure-detector timer) and none of
 // them know whether the user is midway through typing a command line at
 // that exact instant. If they printed directly, a HEARTBEAT or SUSPECT line
 // could land in the middle of the user's half-typed input.
 //
 // So log() only formats and enqueues a line. Only Cli.run() - the one
 // thread that actually owns the terminal - ever calls flush(), and only at
 // points where it knows it isn't waiting on a partially-typed line: right
 // before it blocks on the next readLine(), and right after readLine()
 // unblocks. That's the entire "synchronous logging" mechanism: log lines
 // are always synchronised to a point where the terminal is idle, never
 // interleaved into active typing.
final class PeerLog {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final ConcurrentLinkedQueue<String> pending = new ConcurrentLinkedQueue<>();

    private PeerLog() {
    }

    // Formats a standard "[HH:mm:ss] CATEGORY   message" line and queues it. Never prints directly.
    static void log(String category, String message) {
        String timestamp = LocalTime.now().format(TIME_FORMAT);
        queue(String.format("[%s] %-8s %s", timestamp, category, message));
    }

    // Queues an already-formatted line, e.g. the plain ">> ..." style notices Cli uses for its listener callbacks.
    static void queue(String line) {
        pending.add(line);
    }

         // Writes every currently-queued line to System.out, oldest first, then
     // empties the queue. Must only be called from the thread that owns the
     // terminal (Cli's input loop), and only at a point where that thread
     // itself has no unflushed partial input from the user - see class
     // javadoc.
    static void flush() {
        String line;
        while ((line = pending.poll()) != null) {
            System.out.println(line);
        }
    }
}
