package p2p;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

 // End-to-end smoke test proving Sender, Server, and PeerConfig actually work
 // together over real TCP sockets and real JSON files, ahead of everything
 // being wired into Peer.java. Kept as a permanent regression check since it
 // is fully self-contained (creates its own temp peers.json, its own Server).
 // Run with: java p2p.NetworkingSmokeTest
public class NetworkingSmokeTest {

    public static void main(String[] args) throws Exception {
        testPeerConfigLoadsRealFile();
        testSenderAndServerOverRealSocket();
        System.out.println("\nSMOKE TEST PASSED");
    }

    private static void testPeerConfigLoadsRealFile() throws Exception {
        Path tempFile = Files.createTempFile("peers-smoketest", ".json");
        Files.writeString(tempFile, "[\n"
                + "  { \"id\": \"peer-A\", \"host\": \"localhost\", \"port\": 5001 },\n"
                + "  { \"id\": \"peer-B\", \"host\": \"localhost\", \"port\": 5002 },\n"
                + "  { \"id\": \"peer-C\", \"host\": \"localhost\", \"port\": 5003 }\n"
                + "]\n");

        List<PeerInfo> peers = PeerConfig.load(tempFile);
        assertTrue("expected 3 peers, got " + peers.size(), peers.size() == 3);
        assertTrue("first peer id", peers.get(0).getId().equals("peer-A"));
        assertTrue("first peer port", peers.get(0).getPort() == 5001);
        System.out.println("[PASS] PeerConfig.load parses a real peers.json file: " + peers);

        Files.deleteIfExists(tempFile);
    }

    private static void testSenderAndServerOverRealSocket() throws Exception {
        int port = 15001;
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Message> receivedMessage = new AtomicReference<>();

        Server server = new Server(port, 4, message -> {
            receivedMessage.set(message);
            received.countDown();
        });
        server.start();

        try {
            PeerInfo target = new PeerInfo("peer-B", "localhost", port);
            Message toSend = new Message(MessageType.CHAT, "peer-A", null, "hello over real tcp",
                    Map.of("peer-A", 1, "peer-B", 0), System.currentTimeMillis());

            Sender.send(target, toSend);

            boolean arrived = received.await(3, TimeUnit.SECONDS);
            assertTrue("message should have arrived within 3s", arrived);

            Message got = receivedMessage.get();
            assertTrue("sender id preserved", got.getSenderId().equals("peer-A"));
            assertTrue("body preserved", got.getBody().equals("hello over real tcp"));
            assertTrue("clock preserved", got.getClock().get("peer-A") == 1);

            System.out.println("[PASS] Sender -> Server over a real TCP socket: " + got.toJsonLine());
        } finally {
            server.stop();
        }
    }

    private static void assertTrue(String description, boolean condition) {
        if (!condition) {
            throw new AssertionError("FAILED: " + description);
        }
    }
}
