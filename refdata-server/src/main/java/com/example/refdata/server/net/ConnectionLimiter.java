package com.example.refdata.server.net;

import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.ErrorReply;
import com.example.refdata.server.health.ServerMetrics;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enforces {@code rds.max.connections} (P-2): a connection over the limit gets ErrorReply{SERVER_BUSY, fatal,
 * retry_after_ms} and is closed before any handshake. Shared by all channels.
 */
@ChannelHandler.Sharable
final class ConnectionLimiter extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(ConnectionLimiter.class);

    private final int maxConnections;
    private final int retryAfterMs;
    private final ServerMetrics metrics;
    private final AtomicInteger open = new AtomicInteger();
    /** Set on a channel that was turned away, so that its close is not counted twice. */
    private static final AttributeKey<Boolean> REJECTED = AttributeKey.valueOf("rds.rejected");

    ConnectionLimiter(int maxConnections, int retryAfterMs, ServerMetrics metrics) {
        this.maxConnections = maxConnections;
        this.retryAfterMs = retryAfterMs;
        this.metrics = metrics;
    }

    int open() { return open.get(); }

    @Override public void channelActive(ChannelHandlerContext ctx) {
        int n = open.incrementAndGet();
        if (n > maxConnections) {
            open.decrementAndGet();
            metrics.connectionRejected();
            log.warn("connection limit {} reached, rejecting {}", maxConnections, ctx.channel().remoteAddress());
            ErrorReply err = ErrorReply.newBuilder().setCode(ErrorCode.ERROR_CODE_SERVER_BUSY).setFatal(true).setRetryAfterMs(retryAfterMs)
                    .setMessage("connection limit " + maxConnections + " reached").build();
            ctx.writeAndFlush(Envelope.newBuilder().setError(err).build()).addListener(ChannelFutureListener.CLOSE);
            ctx.channel().attr(REJECTED).set(Boolean.TRUE);
            return;
        }
        ctx.fireChannelActive();
    }

    /** A rejected channel gets nothing past this handler: no Hello reaches the session. */
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (Boolean.TRUE.equals(ctx.channel().attr(REJECTED).get())) { ReferenceCountUtil.release(msg); return; }
        ctx.fireChannelRead(msg);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        if (!Boolean.TRUE.equals(ctx.channel().attr(REJECTED).get())) open.decrementAndGet();
        ctx.fireChannelInactive();
    }
}
