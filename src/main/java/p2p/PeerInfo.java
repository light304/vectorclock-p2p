package p2p;

import java.util.Objects;

// One entry from peers.json: a peer's ID and where to reach it.
public final class PeerInfo {

    private final String id;
    private final String host;
    private final int port;

    public PeerInfo(String id, String host, int port) {
        this.id = Objects.requireNonNull(id, "id");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}
