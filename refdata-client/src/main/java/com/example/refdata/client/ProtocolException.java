package com.example.refdata.client;

/** The peer broke the wire protocol: bad frame, unexpected message, or a fatal error reply that is not a version problem. */
public final class ProtocolException extends RefDataClientException {
    public ProtocolException(String message) { super(message); }
    public ProtocolException(String message, Throwable cause) { super(message, cause); }
}
