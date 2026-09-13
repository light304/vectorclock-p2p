package p2p;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

// Tiny shared logger so every part of the peer (delivery, networking,
// failure detection) prints in the same format. This is not for debugging -
// these log lines ARE the evidence of the vector clock mechanism working,
// so the format is deliberately readable rather than terse.
//
// Format: [HH:mm:ss] CATEGORY   message
final class PeerLog {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private PeerLog() {
    }

    static void log(String category, String message) {
        String timestamp = LocalTime.now().format(TIME_FORMAT);
        System.out.printf("[%s] %-8s %s%n", timestamp, category, message);
    }
}
