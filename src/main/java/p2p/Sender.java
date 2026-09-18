package p2p;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

 // Sends exactly one message to exactly one peer over a short-lived TCP
 // connection: connect, write one newline-terminated JSON line, close. No
 // connection pooling or reuse - at the scale of a student demo (a handful to
 // a few dozen peers) the overhead is irrelevant, and this avoids an entire
 // class of connection-lifecycle bugs.
final class Sender {

    private static final int CONNECT_TIMEOUT_MILLIS = 2000;

    private Sender() {
    }

         // Sends message to target. Throws IOException if the peer can't be
     // reached (not started yet, crashed, wrong address, etc). Callers should
     // expect this to fail routinely - it's a normal, recoverable event.
    static void send(PeerInfo target, Message message) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(target.getHost(), target.getPort()), CONNECT_TIMEOUT_MILLIS);
            OutputStream out = socket.getOutputStream();
            String line = message.toJsonLine() + "\n"; // delimiter is always '\n', written explicitly
            out.write(line.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }
}
