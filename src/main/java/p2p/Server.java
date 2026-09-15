package p2p;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

 // Listens on this peer's port and hands each incoming connection to a small
 // bounded thread pool. Each connection carries exactly one message (see
 // Sender) - read one line, parse it, hand it off, close the socket.
 //
 // Deliberately thread-per-connection rather than NIO/async: correct and easy
 // to reason about at the scale this project targets (a handful to a few
 // dozen peers). Noted in the report as a stated limitation vs. a
 // production-scale design.
final class Server {

    private final int port;
    private final Consumer<Message> onMessage;
    private final ExecutorService connectionPool;
    private volatile ServerSocket serverSocket;
    private volatile boolean running = false;
    private Thread acceptThread;

    Server(int port, int poolSize, Consumer<Message> onMessage) {
        this.port = port;
        this.onMessage = onMessage;
        this.connectionPool = Executors.newFixedThreadPool(Math.max(2, poolSize));
    }

    // Starts listening in a background thread. Returns once the socket is actually bound.
    void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        acceptThread = new Thread(this::acceptLoop, "server-accept-" + port);
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                connectionPool.submit(() -> handleConnection(socket));
            } catch (SocketException e) {
                if (running) {
                    PeerLog.log("ERROR", "Accept loop failed: " + e.getMessage());
                }
                // else: this is just our own socket.close() from stop() - expected, not an error.
            } catch (IOException e) {
                if (running) {
                    PeerLog.log("ERROR", "Accept loop failed: " + e.getMessage());
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket; BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return; // peer connected and disconnected without sending anything - ignore
            }
            Message message = Message.fromJsonLine(line);
            onMessage.accept(message);
        } catch (JsonUtil.JsonParseException e) {
            PeerLog.log("ERROR", "Received malformed message, dropping it: " + e.getMessage());
        } catch (IOException e) {
            PeerLog.log("ERROR", "Connection handling failed: " + e.getMessage());
        }
    }

    // Stops accepting new connections and shuts down the connection thread pool.
    void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // shutting down anyway
        }
        connectionPool.shutdownNow();
    }
}
