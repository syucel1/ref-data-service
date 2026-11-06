package com.example.refdata.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntPredicate;

/**
 * A TCP proxy in front of the server that can misbehave on the server-to-client stream: cut the connection after a
 * number of bytes, flip a byte inside a chosen frame, or throttle. Frames are parsed (4-byte prefix) so a fault can
 * be aimed at one frame; the client-to-server direction is forwarded untouched.
 */
final class TcpProxy implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TcpProxy.class);

    private static final int SMALL_BUFFER = 16 << 10;

    private final ServerSocket listener;
    private final String targetHost;
    private final int targetPort;
    private final CopyOnWriteArrayList<Socket> sockets = new CopyOnWriteArrayList<>();
    private final AtomicInteger connections = new AtomicInteger();
    private volatile long dropAfterBytes = Long.MAX_VALUE;
    private volatile IntPredicate corruptFrame = i -> false;
    private volatile long throttleBytesPerSecond;
    private volatile boolean closed;
    private volatile boolean refusing;

    TcpProxy(String targetHost, int targetPort) throws IOException {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.listener = new ServerSocket(0);
        Thread acceptor = new Thread(this::acceptLoop, "tcp-proxy-" + listener.getLocalPort());
        acceptor.setDaemon(true);
        acceptor.start();
    }

    String endpoint() { return "127.0.0.1:" + listener.getLocalPort(); }
    int connections() { return connections.get(); }

    /** Closes every connection once this many server-to-client bytes have been forwarded on it. */
    TcpProxy dropAfterBytes(long bytes) { this.dropAfterBytes = bytes; return this; }
    /** Flips one byte in the middle of every server-to-client frame whose index (from 0) matches. */
    TcpProxy corruptFrame(IntPredicate frameIndex) { this.corruptFrame = frameIndex; return this; }
    /** While true, new connections are accepted and closed at once (the server looks unreachable). */
    TcpProxy refuse(boolean refuse) { this.refusing = refuse; return this; }

    /** Closes every connection forwarded so far; the proxy keeps accepting new ones. */
    void cutConnections() {
        for (Socket s : sockets) closeQuietly(s);
        sockets.clear();
    }

    /** Limits the server-to-client rate (0 = unlimited). */
    TcpProxy throttle(long bytesPerSecond) { this.throttleBytesPerSecond = bytesPerSecond; return this; }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = listener.accept();
                connections.incrementAndGet();
                if (refusing) { closeQuietly(client); continue; }
                Socket server = new Socket();
                server.setReceiveBufferSize(SMALL_BUFFER);      // keeps the server from pushing a whole snapshot into kernel buffers
                try {
                    server.connect(new InetSocketAddress(targetHost, targetPort));
                } catch (IOException e) {
                    log.info("proxy cannot reach {}:{}; closing the client connection", targetHost, targetPort);
                    closeQuietly(client);
                    continue;
                }
                client.setSendBufferSize(SMALL_BUFFER);
                sockets.add(client);
                sockets.add(server);
                pump("proxy-c2s", client.getInputStream(), server.getOutputStream(), false, server);
                pump("proxy-s2c", server.getInputStream(), client.getOutputStream(), true, client);
            } catch (IOException e) {
                if (!closed) log.warn("proxy accept failed: {}", e.toString());
            }
        }
    }

    /**
     * Forwards one direction. Client-to-server ends with a half-close of the server socket (so bytes the client sent
     * before closing, such as an integrity report, are never lost to a reset); server-to-client ends by closing the client.
     */
    private void pump(String name, InputStream in, OutputStream out, boolean serverToClient, Socket destination) {
        Thread t = new Thread(() -> {
            try {
                if (serverToClient) forwardFrames(in, out); else forwardBytes(in, out);
            } catch (IOException e) {
                log.debug("{} ended: {}", name, e.toString());
            } finally {
                if (serverToClient) closeQuietly(destination);
                else shutdownOutputQuietly(destination);
            }
        }, name);
        t.setDaemon(true);
        t.start();
    }

    private static void forwardBytes(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
            out.flush();
        }
    }

    private void forwardFrames(InputStream in, OutputStream out) throws IOException {
        DataInputStream data = new DataInputStream(in);
        AtomicLong forwarded = new AtomicLong();
        long startNanos = System.nanoTime();
        int index = 0;
        while (true) {
            int length;
            try {
                length = data.readInt();
            } catch (EOFException e) {
                return;
            }
            byte[] body = new byte[length];
            data.readFully(body);
            if (corruptFrame.test(index)) {
                body[length / 2] ^= 0x5a;
                log.info("proxy flipped a byte in frame {} ({} bytes)", index, length);
            }
            index++;
            byte[] frame = new byte[4 + length];
            frame[0] = (byte) (length >>> 24); frame[1] = (byte) (length >>> 16); frame[2] = (byte) (length >>> 8); frame[3] = (byte) length;
            System.arraycopy(body, 0, frame, 4, length);
            long limit = dropAfterBytes;
            int allowed = (int) Math.min(frame.length, Math.max(0, limit - forwarded.get()));
            out.write(frame, 0, allowed);
            out.flush();
            forwarded.addAndGet(allowed);
            if (allowed < frame.length) {
                log.info("proxy cut the connection after {} bytes", forwarded.get());
                throw new EOFException("cut by proxy");
            }
            long rate = throttleBytesPerSecond;
            if (rate > 0) {
                long dueNanos = startNanos + forwarded.get() * 1_000_000_000L / rate;
                long wait = dueNanos - System.nanoTime();
                if (wait > 0) {
                    try { Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000)); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                }
            }
        }
    }

    private static void shutdownOutputQuietly(Socket s) {
        try { s.shutdownOutput(); } catch (IOException ignored) { /* already closed */ }
    }

    private static void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) { /* closing anyway */ }
    }

    @Override public void close() throws IOException {
        closed = true;
        listener.close();
        for (Socket s : sockets) closeQuietly(s);
    }
}
