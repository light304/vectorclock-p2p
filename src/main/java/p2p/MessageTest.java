package p2p;

import java.util.HashMap;
import java.util.Map;

 // Standalone harness for Message + JsonUtil - no networking, no JDK test
 // framework required. Run with: java p2p.MessageTest
public class MessageTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        testBroadcastRoundTrip();
        testDirectMessageRoundTrip();
        testEscapingSpecialCharactersInBody();
        testEmptyClock();
        testMissingFieldIsRejected();
        testUnknownTypeIsRejected();
        testMalformedJsonIsRejected();
        testIsAddressedTo();

        System.out.println();
        System.out.println("---------------------------------------------");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("---------------------------------------------");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void testBroadcastRoundTrip() {
        Map<String, Integer> clock = new HashMap<>();
        clock.put("peer-A", 3);
        clock.put("peer-B", 1);
        clock.put("peer-C", 2);

        Message original = new Message(MessageType.CHAT, "peer-A", null, "hello everyone", clock, 1731045000123L);
        String json = original.toJsonLine();
        Message parsed = Message.fromJsonLine(json);

        check("broadcast: type preserved", parsed.getType() == MessageType.CHAT);
        check("broadcast: senderId preserved", parsed.getSenderId().equals("peer-A"));
        check("broadcast: targetId is null (broadcast)", parsed.getTargetId() == null);
        check("broadcast: isBroadcast() true", parsed.isBroadcast());
        check("broadcast: body preserved", parsed.getBody().equals("hello everyone"));
        check("broadcast: clock preserved", parsed.getClock().equals(clock));
        check("broadcast: sentAtMillis preserved", parsed.getSentAtMillis() == 1731045000123L);
    }

    private static void testDirectMessageRoundTrip() {
        Map<String, Integer> clock = new HashMap<>();
        clock.put("peer-A", 5);

        Message original = new Message(MessageType.CHAT, "peer-A", "peer-B", "just for you", clock, 1000L);
        Message parsed = Message.fromJsonLine(original.toJsonLine());

        check("direct: targetId preserved", "peer-B".equals(parsed.getTargetId()));
        check("direct: isBroadcast() false", !parsed.isBroadcast());
    }

    private static void testEscapingSpecialCharactersInBody() {
        Map<String, Integer> clock = new HashMap<>();
        clock.put("peer-A", 1);

        String trickyBody = "she said \"hello\"\nand a tab:\tand a backslash: \\ done";
        Message original = new Message(MessageType.CHAT, "peer-A", null, trickyBody, clock, 1L);
        Message parsed = Message.fromJsonLine(original.toJsonLine());

        check("escaping: quotes/newlines/tabs/backslashes survive a round trip",
                parsed.getBody().equals(trickyBody));
    }

    private static void testEmptyClock() {
        Message original = new Message(MessageType.JOIN, "peer-A", null, "", new HashMap<>(), 1L);
        Message parsed = Message.fromJsonLine(original.toJsonLine());

        check("empty clock: round-trips to an empty map", parsed.getClock().isEmpty());
    }

    private static void testMissingFieldIsRejected() {
        String brokenJson = "{\"type\":\"CHAT\",\"senderId\":\"peer-A\"}"; // missing targetId/body/clock/sentAtMillis
        boolean threw = false;
        try {
            Message.fromJsonLine(brokenJson);
        } catch (JsonUtil.JsonParseException expected) {
            threw = true;
        }
        check("validation: missing required field is rejected", threw);
    }

    private static void testUnknownTypeIsRejected() {
        String brokenJson = "{\"type\":\"NOT_A_REAL_TYPE\",\"senderId\":\"peer-A\",\"targetId\":null,"
                + "\"body\":\"\",\"clock\":{},\"sentAtMillis\":1}";
        boolean threw = false;
        try {
            Message.fromJsonLine(brokenJson);
        } catch (JsonUtil.JsonParseException expected) {
            threw = true;
        }
        check("validation: unknown message type is rejected", threw);
    }

    private static void testMalformedJsonIsRejected() {
        String brokenJson = "{\"type\":\"CHAT\", this is not valid json at all";
        boolean threw = false;
        try {
            Message.fromJsonLine(brokenJson);
        } catch (JsonUtil.JsonParseException expected) {
            threw = true;
        }
        check("validation: malformed JSON is rejected cleanly (no crash)", threw);
    }

    private static void testIsAddressedTo() {
        Map<String, Integer> clock = new HashMap<>();
        Message broadcast = new Message(MessageType.CHAT, "peer-A", null, "hi", clock, 1L);
        Message direct = new Message(MessageType.CHAT, "peer-A", "peer-B", "hi", clock, 1L);

        check("addressing: broadcast is addressed to everyone (peer-B)", broadcast.isAddressedTo("peer-B"));
        check("addressing: broadcast is addressed to everyone (peer-C)", broadcast.isAddressedTo("peer-C"));
        check("addressing: direct message is addressed to its target", direct.isAddressedTo("peer-B"));
        check("addressing: direct message is NOT addressed to a bystander", !direct.isAddressedTo("peer-C"));
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
