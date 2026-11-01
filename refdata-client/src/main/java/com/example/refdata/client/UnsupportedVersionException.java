package com.example.refdata.client;

import com.example.refdata.protocol.v1.ErrorReply;

/** The server rejected the handshake: protocol major, schema major or frame size are incompatible (fatal, never retried). */
public final class UnsupportedVersionException extends RefDataClientException {
    private final transient ErrorReply reply;

    public UnsupportedVersionException(String message, ErrorReply reply) {
        super(message);
        this.reply = reply;
    }

    /** The server's reply, which carries the protocol and schema versions it does support. */
    public ErrorReply reply() { return reply; }
}
