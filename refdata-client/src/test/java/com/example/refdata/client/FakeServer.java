package com.example.refdata.client;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scripted stand-in for the server: listens on an ephemeral port and runs one {@link ConnectionScript} per accepted
 * connection, in the order they were queued with {@link #onConnection}. A connection with no script left is closed at
 * once and counted. Anything a script throws (assertion errors included) is collected in {@link #failures()}.
 */
public final class FakeServer implements AutoCloseable {
    /** What a script can do with one connection. Frames are read with the server's inbound limit. */
    public interface Session {
        Envelope read() throws IOException;
        void send(Envelope frame) throws IOException;
        /** Reads frames until the client closes the connection; returns what was read. */
        List<Envelope> drain();
    }

    /** The server side of one connection. Returning closes the connection. */
    @FunctionalInterface
    public interface ConnectionScript {
        void run(Session session) throws Exception;
    }

    private final ServerSocket serverSocket;
    private final Queue<ConnectionScript> scripts = new ConcurrentLinkedQueue<>();
    private final List<Throwable> failures = new CopyOnWriteArrayList<>();
    private final AtomicInteger connections = new AtomicInteger();
    private final AtomicInteger unscripted = new AtomicInteger();
    private final List<Thread> workers = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    public FakeServer() throws IOException {
        serverSocket = new ServerSocket(0);
        Thread acceptor = new Thread(this::acceptLoop, "fake-server-" + serverSocket.getLocalPort());
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public int port() { return serverSocket.getLocalPort(); }
    public String endpoint() { return "127.0.0.1:" + port(); }
    /** Connections accepted so far, scripted or not. */
    public int connections() { return connections.get(); }
    /** Connections that arrived after the scripts ran out (closed immediately). */
    public int unscriptedConnections() { return unscripted.get(); }
    public List<Throwable> failures() { return failures; }
    /** Whether every queued script has been started. */
    public boolean allScriptsUsed() { return scripts.isEmpty(); }

    public FakeServer onConnection(ConnectionScript script) {
        scripts.add(script);
        return this;
    }

    /** Waits for the running scripts to finish, then fails the test if any threw. */
    public void assertNoFailures() throws InterruptedException {
        for (Thread w : workers) w.join(10_000);
        if (!failures.isEmpty()) {
            AssertionError e = new AssertionError("fake server script failed: " + failures.get(0));
            e.initCause(failures.get(0));
            throw e;
        }
    }

    private void acceptLoop() {
        while (!closed) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                return;
            }
            connections.incrementAndGet();
            ConnectionScript script = scripts.poll();
            if (script == null) {
                unscripted.incrementAndGet();
                closeQuietly(socket);
                continue;
            }
            Thread worker = new Thread(() -> run(socket, script), "fake-server-session");
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }
    }

    private void run(Socket socket, ConnectionScript script) {
        try (socket) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(10_000);
            FrameIO frames = new FrameIO(socket.getInputStream(), socket.getOutputStream(), Framing.SERVER_INBOUND_MAX_FRAME_BYTES);
            script.run(new Session() {
                @Override public Envelope read() throws IOException { return frames.read(); }
                @Override public void send(Envelope frame) throws IOException { frames.write(frame); }
                @Override public List<Envelope> drain() {
                    List<Envelope> out = new ArrayList<>();
                    try {
                        while (true) out.add(frames.read());
                    } catch (IOException e) {
                        return out;
                    }
                }
            });
        } catch (Throwable t) {
            failures.add(t);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    @Override public void close() throws IOException {
        closed = true;
        serverSocket.close();
    }
}
