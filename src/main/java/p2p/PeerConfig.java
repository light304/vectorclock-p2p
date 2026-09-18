package p2p;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

 // Loads the startup peer list from a JSON file such as:
 // [
 // { "id": "peer-A", "host": "localhost", "port": 5001 },
 // { "id": "peer-B", "host": "localhost", "port": 5002 }
 // ]
 // The list can be any length - adding a peer to the network means adding one
 // entry here, no code changes required anywhere else.
public final class PeerConfig {

    private PeerConfig() {
    }

    public static List<PeerInfo> load(Path path) throws IOException {
        String json = Files.readString(path, StandardCharsets.UTF_8);
        List<Map<String, Object>> rawEntries;
        try {
            rawEntries = JsonUtil.parseArrayOfFlatObjects(json);
        } catch (JsonUtil.JsonParseException e) {
            throw new IOException("Could not parse " + path + " as a JSON array of peer objects: " + e.getMessage(), e);
        }

        List<PeerInfo> peers = new ArrayList<>();
        for (Map<String, Object> entry : rawEntries) {
            String id = requireString(entry, "id", path);
            String host = requireString(entry, "host", path);
            int port = requireInt(entry, "port", path);
            peers.add(new PeerInfo(id, host, port));
        }

        if (peers.isEmpty()) {
            throw new IOException(path + " contains no peers");
        }
        return peers;
    }

    private static String requireString(Map<String, Object> entry, String field, Path path) throws IOException {
        Object value = entry.get(field);
        if (!(value instanceof String)) {
            throw new IOException("Peer entry in " + path + " is missing a string '" + field + "' field: " + entry);
        }
        return (String) value;
    }

    private static int requireInt(Map<String, Object> entry, String field, Path path) throws IOException {
        Object value = entry.get(field);
        if (!(value instanceof String)) { // JsonUtil returns raw numbers as their string form
            throw new IOException("Peer entry in " + path + " is missing a numeric '" + field + "' field: " + entry);
        }
        try {
            return Integer.parseInt((String) value);
        } catch (NumberFormatException e) {
            throw new IOException("Peer entry in " + path + " has an invalid '" + field + "' value: " + value);
        }
    }
}
