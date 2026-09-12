package p2p;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

 // A deliberately small JSON encoder/decoder - not a general-purpose library.
 // It knows how to handle exactly what Message needs: a flat JSON object whose
 // values are strings, whole numbers, null, or one level of nested
 // {peerId: count} object (for the vector clock). That's the entire message
 // format for this project, so a full JSON library would be solving a bigger
 // problem than we actually have.
 //
 // Kept dependency-free on purpose: the professor only needs a JDK to build
 // and run this, not a working internet connection to pull a Maven artifact.
final class JsonUtil {

    private JsonUtil() {
    }

    // ---------- Writing ----------

    // Writes a string field as a properly-escaped, quoted JSON string.
    static String writeString(String value) {
        StringBuilder out = new StringBuilder();
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':  out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        out.append('"');
        return out.toString();
    }

    // Writes a {peerId: count} map as a JSON object, sorted by key for stable/readable output.
    static String writeIntMap(Map<String, Integer> map) {
        StringBuilder out = new StringBuilder();
        out.append('{');
        boolean first = true;
        for (Map.Entry<String, Integer> entry : new TreeMap<>(map).entrySet()) {
            if (!first) out.append(',');
            first = false;
            out.append(writeString(entry.getKey())).append(':').append(entry.getValue());
        }
        out.append('}');
        return out.toString();
    }

    // ---------- Reading ----------

         // Parses a flat JSON object (optionally containing one nested int-valued
     // object) into a Map<String,Object>. Nested objects come back as
     // Map<String,Integer>; everything else comes back as String or null.
     // Numbers are returned as their original string form for the top level
     // (Message parses sentAtMillis itself) to keep this class simple.
    static Map<String, Object> parseObject(String json) {
        Parser parser = new Parser(json);
        parser.skipWhitespace();
        Map<String, Object> result = parser.parseObjectBody(true);
        parser.skipWhitespace();
        if (!parser.atEnd()) {
            throw new JsonParseException("Unexpected trailing content at position " + parser.pos);
        }
        return result;
    }

         // Parses a top-level JSON array of flat objects (no nesting) - exactly
     // the shape of peers.json: [ {"id": "...", "host": "...", "port": 5001}, ... ]
    static List<Map<String, Object>> parseArrayOfFlatObjects(String json) {
        Parser parser = new Parser(json);
        parser.skipWhitespace();
        List<Map<String, Object>> result = parser.parseArray();
        parser.skipWhitespace();
        if (!parser.atEnd()) {
            throw new JsonParseException("Unexpected trailing content at position " + parser.pos);
        }
        return result;
    }

    static class JsonParseException extends RuntimeException {
        JsonParseException(String message) {
            super(message);
        }
    }

    private static class Parser {
        private final String text;
        private int pos = 0;

        Parser(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return pos >= text.length();
        }

        void skipWhitespace() {
            while (!atEnd() && Character.isWhitespace(text.charAt(pos))) pos++;
        }

        char peek() {
            if (atEnd()) throw new JsonParseException("Unexpected end of input at position " + pos);
            return text.charAt(pos);
        }

        void expect(char c) {
            if (atEnd() || text.charAt(pos) != c) {
                throw new JsonParseException("Expected '" + c + "' at position " + pos);
            }
            pos++;
        }

        List<Map<String, Object>> parseArray() {
            List<Map<String, Object>> list = new ArrayList<>();
            expect('[');
            skipWhitespace();
            if (!atEnd() && peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                skipWhitespace();
                list.add(parseObjectBody(false)); // flat objects only - matches peers.json
                skipWhitespace();
                if (!atEnd() && peek() == ',') {
                    pos++;
                    continue;
                }
                break;
            }
            skipWhitespace();
            expect(']');
            return list;
        }

        Map<String, Object> parseObjectBody(boolean allowNested) {
            Map<String, Object> map = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if (!atEnd() && peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                Object value = parseValue(allowNested);
                map.put(key, value);
                skipWhitespace();
                if (!atEnd() && peek() == ',') {
                    pos++;
                    continue;
                }
                break;
            }
            skipWhitespace();
            expect('}');
            return map;
        }

        Object parseValue(boolean allowNested) {
            skipWhitespace();
            char c = peek();
            if (c == '"') {
                return parseString();
            }
            if (c == '{') {
                if (!allowNested) {
                    throw new JsonParseException("Unexpected nested object at position " + pos);
                }
                return parseIntObjectBody();
            }
            if (c == 'n') {
                expectLiteral("null");
                return null;
            }
            if (c == '-' || Character.isDigit(c)) {
                return parseNumberAsString();
            }
            throw new JsonParseException("Unexpected character '" + c + "' at position " + pos);
        }

        Map<String, Integer> parseIntObjectBody() {
            Map<String, Integer> map = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if (!atEnd() && peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                String numberText = parseNumberAsString();
                map.put(key, Integer.parseInt(numberText));
                skipWhitespace();
                if (!atEnd() && peek() == ',') {
                    pos++;
                    continue;
                }
                break;
            }
            skipWhitespace();
            expect('}');
            return map;
        }

        String parseString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (true) {
                if (atEnd()) throw new JsonParseException("Unterminated string starting near position " + pos);
                char c = text.charAt(pos++);
                if (c == '"') break;
                if (c == '\\') {
                    if (atEnd()) throw new JsonParseException("Unterminated escape sequence");
                    char escaped = text.charAt(pos++);
                    switch (escaped) {
                        case '"':  out.append('"'); break;
                        case '\\': out.append('\\'); break;
                        case '/':  out.append('/'); break;
                        case 'n':  out.append('\n'); break;
                        case 'r':  out.append('\r'); break;
                        case 't':  out.append('\t'); break;
                        case 'u':
                            if (pos + 4 > text.length()) throw new JsonParseException("Truncated \\u escape");
                            String hex = text.substring(pos, pos + 4);
                            out.append((char) Integer.parseInt(hex, 16));
                            pos += 4;
                            break;
                        default:
                            throw new JsonParseException("Unknown escape '\\" + escaped + "'");
                    }
                } else {
                    out.append(c);
                }
            }
            return out.toString();
        }

        String parseNumberAsString() {
            int start = pos;
            if (!atEnd() && peek() == '-') pos++;
            while (!atEnd() && Character.isDigit(peek())) pos++;
            if (start == pos) throw new JsonParseException("Invalid number at position " + pos);
            return text.substring(start, pos);
        }

        void expectLiteral(String literal) {
            if (pos + literal.length() > text.length() || !text.startsWith(literal, pos)) {
                throw new JsonParseException("Expected literal '" + literal + "' at position " + pos);
            }
            pos += literal.length();
        }
    }
}
