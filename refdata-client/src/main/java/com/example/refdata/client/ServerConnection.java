package com.example.refdata.client;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.ErrorReply;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.HelloAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

/** One TCP connection to one server: blocking socket I/O plus the handshake (design section 9.4). */
final class ServerConnection implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ServerConnection.class);

    /** How long the client keeps reading after an integrity report so that the report reaches the server. */
    private static final Duration REPORT_DRAIN = Duration.ofMillis(250);

    private final Endpoint endpoint;
    private final Socket socket;
    private final FrameIO frames;

    private ServerConnection(Endpoint endpoint, Socket socket, int maxFrameBytes) throws IOException {
        this.endpoint = endpoint;
        this.socket = socket;
        this.frames = new FrameIO(socket.getInputStream(), socket.getOutputStream(), maxFrameBytes);
    }

    static ServerConnection open(Endpoint endpoint, Duration connectTimeout, Duration readTimeout, int maxFrameBytes) throws IOException {
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(millis(readTimeout));
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), millis(connectTimeout));
            return new ServerConnection(endpoint, socket, maxFrameBytes);
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
    }

    Endpoint endpoint() { return endpoint; }

    /** Reads block at most this long; a longer silence is reported as an IOException. */
    void readTimeout(Duration timeout) throws IOException { socket.setSoTimeout(millis(timeout)); }

    Envelope read() throws IOException { return frames.read(); }

    /** Writes one frame; synchronized because the keep-alive sender and the loader may share the connection. */
    synchronized void send(Envelope envelope) throws IOException { frames.write(envelope); }

    long bytesRead() { return frames.bytesRead(); }

    /**
     * Sends Hello and waits for HelloAck.
     * @throws UnsupportedVersionException when the server rejects our protocol/schema version or frame limit (fatal)
     * @throws ServerBusyException         when the server has no room for another connection
     * @throws ProtocolException           on any other error reply or an unexpected message
     */
    HelloAck handshake(Hello hello) throws IOException {
        send(Envelope.newBuilder().setHello(hello).build());
        while (true) {
            Envelope reply = read();
            switch (reply.getBodyCase()) {
                case HELLO_ACK -> {
                    HelloAck ack = reply.getHelloAck();
                    if (ack.getProtocolMajor() != Framing.PROTOCOL_MAJOR) {
                        throw new ProtocolException("server answered with protocol major " + ack.getProtocolMajor());
                    }
                    if (!hello.getSchemaMajorsList().contains(ack.getSchemaMajor())) {
                        throw new ProtocolException("server chose schema major " + ack.getSchemaMajor() + ", which we did not offer");
                    }
                    if (ack.getMaxFrameBytes() > hello.getMaxFrameBytes()) {
                        throw new ProtocolException("server announces frames of " + ack.getMaxFrameBytes() + " bytes, above our limit " + hello.getMaxFrameBytes());
                    }
                    return ack;
                }
                case HEARTBEAT -> log.debug("heartbeat during handshake ignored");
                case ERROR -> throw handshakeError(reply.getError());
                default -> throw new ProtocolException("expected HelloAck, got " + reply.getBodyCase());
            }
        }
    }

    private static RefDataClientException handshakeError(ErrorReply error) {
        String text = error.getCode() + ": " + error.getMessage();
        return switch (error.getCode()) {
            case ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, ERROR_CODE_UNSUPPORTED_SCHEMA_VERSION, ERROR_CODE_FRAME_TOO_LARGE ->
                    new UnsupportedVersionException("handshake rejected, " + text + " (server supports protocol "
                            + error.getSupportedProtocolMin() + ".." + error.getSupportedProtocolMax() + ", schema majors "
                            + error.getSupportedSchemaMajorsList() + ")", error);
            case ERROR_CODE_SERVER_BUSY -> new ServerBusyException(text, Duration.ofMillis(error.getRetryAfterMs()));
            default -> new ProtocolException("handshake failed, " + text);
        };
    }

    /**
     * Reports a chunk that failed its CRC or count check (design row C-3), then half-closes and briefly drains the
     * stream, so the report is not lost to a TCP reset caused by closing with unread chunks. Failures are only logged.
     */
    void reportIntegrityFailure(long requestId, int seq, String message) {
        ErrorReply report = ErrorReply.newBuilder().setRequestId(requestId).setCode(ErrorCode.ERROR_CODE_INTEGRITY_FAILURE)
                .setMessage(message).setFatal(true).setSeq(seq).build();
        try {
            send(Envelope.newBuilder().setError(report).build());
            socket.shutdownOutput();
            socket.setSoTimeout((int) REPORT_DRAIN.toMillis());
            long until = System.nanoTime() + REPORT_DRAIN.toNanos();
            byte[] sink = new byte[64 << 10];
            while (System.nanoTime() < until && socket.getInputStream().read(sink) >= 0) {
                // discarding whatever the server still had in flight
            }
        } catch (IOException e) {
            log.debug("after reporting integrity failure of chunk {} to {}: {}", seq, endpoint, e.toString());
        }
    }

    @Override public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("closing connection to {}: {}", endpoint, e.toString());
        }
    }

    private static int millis(Duration d) { return (int) Math.min(Integer.MAX_VALUE, Math.max(1, d.toMillis())); }
}
