package com.example.refdata.server.health;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A minimal JSON writer for the health endpoints: nested objects and arrays, string/number/boolean members.
 * Inside an object call {@link #key} (or the typed shortcuts) before each value; inside an array call the value
 * methods directly.
 */
final class Json {
    private final StringBuilder sb = new StringBuilder(512);
    /** One entry per open scope: the closing bracket and how many members were written. */
    private final Deque<Scope> scopes = new ArrayDeque<>();
    private boolean afterKey;

    private static final class Scope {
        final char close;
        int members;
        Scope(char close) { this.close = close; }
    }

    Json obj() { open('{', '}'); return this; }
    Json arr() { open('[', ']'); return this; }

    Json end() {
        Scope s = scopes.pop();
        sb.append(s.close);
        return this;
    }

    Json key(String k) {
        separator();
        quote(k);
        sb.append(':');
        afterKey = true;
        return this;
    }

    Json str(String k, String v) { key(k); return str(v); }
    Json num(String k, long v) { key(k); return num(v); }
    Json bool(String k, boolean v) { key(k); return bool(v); }

    Json str(String v) { separator(); quote(v); return this; }
    Json num(long v) { separator(); sb.append(v); return this; }
    Json bool(boolean v) { separator(); sb.append(v); return this; }

    @Override public String toString() { return sb.toString(); }

    private void open(char open, char close) {
        separator();
        sb.append(open);
        scopes.push(new Scope(close));
    }

    /** Writes the comma before a member unless it is the first of its scope or follows a key. */
    private void separator() {
        if (afterKey) { afterKey = false; return; }
        Scope s = scopes.peek();
        if (s == null) return;
        if (s.members++ > 0) sb.append(',');
    }

    private void quote(String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> { if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c); }
            }
        }
        sb.append('"');
    }
}
