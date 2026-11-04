package com.example.refdata.server.net;

import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.server.ServerConfig;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.snapshot.Snapshot;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The Netty acceptor: one boss thread, {@code rds.io.threads} I/O threads, the connection limit, the frame codec
 * and one {@link SessionHandler} per connection. {@link #shutdown()} implements the planned-shutdown sequence
 * (GoAway to every session, stop accepting, drain running transfers, close).
 */
public final class RefDataTcpServer implements AutoCloseable, SessionHandler.SessionHost {
    private static final Logger log = LoggerFactory.getLogger(RefDataTcpServer.class);
    /** Hint sent with GoAway and connection-limit rejections. */
    private static final int RECONNECT_AFTER_MS = 2_000;
    /** Published updates kept for live sessions that are still catching up; one further behind must reload. */
    static final int LIVE_UPDATE_HISTORY = 32;

    private final ServerConfig config;
    private volatile Snapshot snapshot;
    /** The last {@link #LIVE_UPDATE_HISTORY} updates, oldest first; replaced (never mutated) on each publication. */
    private volatile List<SessionHandler.LiveUpdate> liveUpdates = List.of();
    private final ServerMetrics metrics;
    private final String serverId;
    private final String serverVersion;
    private final AdmissionController admission;
    private final ConnectionLimiter limiter;
    private final EnvelopeCodec.Encoder encoder = new EnvelopeCodec.Encoder();
    private final ChannelGroup sessions = new DefaultChannelGroup("rds-sessions", GlobalEventExecutor.INSTANCE);
    private EventLoopGroup bossGroup, workerGroup;
    private Channel serverChannel;
    private volatile boolean acceptingTransfers = true;
    private volatile Runnable degradedListener = () -> { };

    public RefDataTcpServer(ServerConfig config, Snapshot snapshot, ServerMetrics metrics, String serverId, String serverVersion) {
        this.config = config;
        this.snapshot = snapshot;
        this.metrics = metrics;
        this.serverId = serverId;
        this.serverVersion = serverVersion;
        this.admission = new AdmissionController(config.maxConcurrentSnapshots, config.snapshotQueueSize);
        this.limiter = new ConnectionLimiter(config.maxConnections, RECONNECT_AFTER_MS, metrics);
    }

    /** Binds {@code rds.tcp.bind.address:rds.tcp.port} (port 0 = any free port) and starts accepting. */
    public synchronized void start() throws InterruptedException {
        if (serverChannel != null) throw new IllegalStateException("already started");
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(config.ioThreads, NioIoHandler.newFactory());
        ServerBootstrap b = new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 256).option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(config.writeBufferLowWater, config.writeBufferHighWater))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        sessions.add(ch);
                        ch.pipeline().addLast("encoder", encoder)
                                .addLast("limiter", limiter)
                                .addLast("frameDecoder", new FrameDecoder())
                                .addLast("envelopeDecoder", new EnvelopeCodec.Decoder())
                                .addLast("session", new SessionHandler(config, snapshot, admission, RefDataTcpServer.this, metrics, serverId, serverVersion));
                    }
                });
        if (config.tcpSendBuffer > 0) b.childOption(ChannelOption.SO_SNDBUF, config.tcpSendBuffer);
        try {
            serverChannel = b.bind(new InetSocketAddress(config.bindAddress, config.tcpPort)).sync().channel();
        } catch (Exception e) {
            bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
            workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
            throw new IllegalStateException("cannot bind TCP port " + config.tcpPort + " on " + config.bindAddress + ": " + e.getMessage(), e);
        }
        log.info("TCP server listening on {} (io threads {}, max connections {}, max concurrent snapshots {}, queue {})",
                serverChannel.localAddress(), config.ioThreads, config.maxConnections, config.maxConcurrentSnapshots, config.snapshotQueueSize);
    }

    /** The bound port (useful when configured as 0). */
    public synchronized int port() {
        if (serverChannel == null) throw new IllegalStateException("not started");
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /**
     * Publishes the next data version: new connections get {@code next}; live sessions get {@code update} (an
     * {@code update_batch} envelope moving from {@code next.dataVersion() - 1}) pushed to them at once.
     * The update is recorded before the snapshot is switched, so a session never misses the step between the
     * snapshot it was given and the updates after it.
     */
    public synchronized void publishUpdate(Snapshot next, Envelope update) {
        List<SessionHandler.LiveUpdate> retained = new ArrayList<>(liveUpdates);
        retained.add(new SessionHandler.LiveUpdate(next.dataVersion() - 1, next.dataVersion(), update));
        if (retained.size() > LIVE_UPDATE_HISTORY) retained.remove(0);
        liveUpdates = List.copyOf(retained);
        snapshot = next;
        for (Channel ch : sessions) {
            SessionHandler s = ch.pipeline().get(SessionHandler.class);
            if (s != null) s.deliverLiveUpdates();
        }
    }

    @Override public List<SessionHandler.LiveUpdate> updatesAfter(long afterVersion) {
        List<SessionHandler.LiveUpdate> all = liveUpdates;
        int from = 0;
        while (from < all.size() && all.get(from).version() <= afterVersion) from++;
        return all.subList(from, all.size());
    }

    @Override public boolean liveUpdatesEnabled() { return true; }
    public AdmissionController admission() { return admission; }

    /** Called once when a stored frame is found corrupt (design S-16): the owner should mark the instance not ready. */
    public void onDegraded(Runnable listener) { this.degradedListener = listener; }

    @Override public boolean acceptingTransfers() { return acceptingTransfers; }

    @Override public void storedFrameCorrupt(int seq, Codec codec) {
        if (!acceptingTransfers) return;
        acceptingTransfers = false;
        log.error("stored chunk {} ({}) failed its CRC re-check: no new transfers will be admitted, readiness goes DOWN - restart this instance", seq, codec);
        degradedListener.run();
    }
    public int openConnections() { return limiter.open(); }

    /**
     * Planned shutdown (P-17): stop accepting, GoAway to every session, wait up to {@code rds.shutdown.drain.timeout}
     * for running transfers, then close everything. Safe to call more than once.
     */
    public synchronized void shutdown() {
        if (serverChannel == null) return;
        log.info("shutting down: stop accepting, GoAway to {} session(s), drain up to {} ms", sessions.size(), config.shutdownDrainTimeout.toMillis());
        serverChannel.close().syncUninterruptibly();
        serverChannel = null;
        for (Channel ch : sessions) {
            SessionHandler s = ch.pipeline().get(SessionHandler.class);
            if (s != null) ch.eventLoop().execute(() -> s.goAway(RECONNECT_AFTER_MS));
        }
        long deadline = System.nanoTime() + config.shutdownDrainTimeout.toNanos();
        while (anyStreaming() && System.nanoTime() < deadline) {
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        if (anyStreaming()) log.warn("drain timeout: closing {} session(s) with transfers in progress", sessions.size());
        sessions.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
        workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        log.info("TCP server stopped");
    }

    private boolean anyStreaming() {
        for (Channel ch : sessions) {
            SessionHandler s = ch.pipeline().get(SessionHandler.class);
            if (s != null && s.streaming()) return true;
        }
        return false;
    }

    @Override public void close() { shutdown(); }
}
