package com.example.refdata.client;

/** A server address. */
public record Endpoint(String host, int port) {
    public Endpoint {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host is blank");
        if (port < 1 || port > 65535) throw new IllegalArgumentException("port out of range: " + port);
    }

    /** Parses {@code host:port}. */
    public static Endpoint parse(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 1 || colon == hostPort.length() - 1) throw new IllegalArgumentException("expected host:port, got '" + hostPort + "'");
        try {
            return new Endpoint(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad port in '" + hostPort + "'", e);
        }
    }

    @Override public String toString() { return host + ":" + port; }
}
