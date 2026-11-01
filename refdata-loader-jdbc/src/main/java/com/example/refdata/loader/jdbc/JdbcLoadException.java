package com.example.refdata.loader.jdbc;

/** A load or pre-flight failed; the message names the table or step and the underlying cause is attached. */
public final class JdbcLoadException extends Exception {
    public JdbcLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbcLoadException(String message) {
        super(message);
    }
}
