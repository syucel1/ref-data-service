package com.example.refdata.server.net;

import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.Hello;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

/** A tiny raw protocol client for socket-level tests: plain java.net.Socket, writes Envelopes, reads frames. */
final class RawClient implements AutoCloseable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    RawClient(int port, int readTimeoutMs) throws IOException {
        this(port, readTimeoutMs, 0);
    }

    /** @param receiveBuffer SO_RCVBUF to set before connecting (small = a stalled reader blocks the server sooner); 0 = default */
    RawClient(int port, int readTimeoutMs, int receiveBuffer) throws IOException {
        socket = new Socket();
        if (receiveBuffer > 0) socket.setReceiveBufferSize(receiveBuffer);
        socket.connect(new java.net.InetSocketAddress("127.0.0.1", port));
        socket.setSoTimeout(readTimeoutMs);
        socket.setTcpNoDelay(true);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
    }

    void send(Envelope e) throws IOException {
        byte[] b = e.toByteArray();
        out.writeInt(b.length);
        out.write(b);
        out.flush();
    }

    void sendRaw(byte[] frame) throws IOException {
        out.write(frame);
        out.flush();
    }

    /** Next frame as an Envelope; null at end of stream. */
    Envelope read() throws IOException {
        int len;
        try { len = in.readInt(); } catch (EOFException e) { return null; }
        byte[] b = new byte[len];
        in.readFully(b);
        return Envelope.parseFrom(b);
    }

    /** Reads frames, skipping heartbeats, until one of another kind (or end of stream). */
    Envelope readSkippingHeartbeats() throws IOException {
        Envelope e;
        do { e = read(); } while (e != null && e.getBodyCase() == Envelope.BodyCase.HEARTBEAT);
        return e;
    }

    static Envelope hello(String name, boolean keepAlive) {
        return Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName(name)
                .setClientInstance("test-1").setLibraryVersion("raw").setKeepAlive(keepAlive)).build();
    }

    Envelope handshake(String name) throws IOException {
        send(hello(name, false));
        return read();
    }

    boolean isClosedByPeer() throws IOException {
        try { return in.read() < 0; } catch (java.net.SocketTimeoutException e) { return false; }
    }

    @Override public void close() throws IOException { socket.close(); }
}
