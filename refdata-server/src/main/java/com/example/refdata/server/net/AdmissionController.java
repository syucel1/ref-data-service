package com.example.refdata.server.net;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Admission control for snapshot transfers (design 9.6, P-10): at most {@code maxConcurrent} run at once; further
 * requests wait in a bounded FIFO queue. Thread-safe; the callbacks run on the caller's thread of {@link #release()},
 * so waiters must hop to their own event loop.
 */
public final class AdmissionController {
    /** A queued transfer. */
    public interface Waiter {
        /** A slot has been granted to this waiter (it now counts as active until {@link #release()}). */
        void admitted();
    }

    /** Outcome of {@link #request}: the slot is granted, the waiter is queued at {@code position} (1 = next), or neither. */
    public enum Outcome { ADMITTED, QUEUED, BUSY }

    public record Decision(Outcome outcome, int position) {
    }

    private final int maxConcurrent;
    private final int queueSize;
    private final Deque<Waiter> queue = new ArrayDeque<>();
    private int active;

    public AdmissionController(int maxConcurrent, int queueSize) {
        if (maxConcurrent < 1) throw new IllegalArgumentException("maxConcurrent must be >= 1");
        if (queueSize < 0) throw new IllegalArgumentException("queueSize must be >= 0");
        this.maxConcurrent = maxConcurrent;
        this.queueSize = queueSize;
    }

    /** Asks for a transfer slot. ADMITTED means the waiter holds a slot now and must {@link #release()} it. */
    public synchronized Decision request(Waiter w) {
        if (active < maxConcurrent) {
            active++;
            return new Decision(Outcome.ADMITTED, 0);
        }
        if (queue.size() < queueSize) {
            queue.addLast(w);
            return new Decision(Outcome.QUEUED, queue.size());
        }
        return new Decision(Outcome.BUSY, 0);
    }

    /** Removes a queued waiter; false when it is not queued (already admitted, or never queued). */
    public synchronized boolean cancel(Waiter w) {
        return queue.remove(w);
    }

    /** Gives a slot back; the next queued waiter, if any, is admitted and told so. */
    public void release() {
        Waiter next;
        synchronized (this) {
            if (active <= 0) throw new IllegalStateException("release without a held slot");
            next = queue.pollFirst();
            if (next == null) active--;
        }
        if (next != null) next.admitted();
    }

    public synchronized int active() { return active; }
    public synchronized int queued() { return queue.size(); }
    public int maxConcurrent() { return maxConcurrent; }
    public int queueSize() { return queueSize; }
}
