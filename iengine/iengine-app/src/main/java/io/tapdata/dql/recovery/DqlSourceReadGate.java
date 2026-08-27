package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates normal source enqueues with a DQL recovery batch.
 *
 * <p>The gate deliberately lives at the source enqueue boundary. It does not
 * change the task's business status and it does not stop the source reader
 * itself; it only prevents new normal events from entering the source queue
 * while recovery owns the processing path.</p>
 */
public final class DqlSourceReadGate {
    public enum State {
        OPEN,
        PAUSING,
        RECOVERY_ONLY,
        RESUMING
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition drained = lock.newCondition();
    private final Set<TapdataEvent> inFlightNormalEvents =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private State state = State.OPEN;

    public void open() {
        lock.lock();
        try {
            state = State.OPEN;
            if (inFlightNormalEvents.isEmpty()) {
                drained.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void beginPausing() {
        lock.lock();
        try {
            requireState(State.OPEN, "begin pausing");
            state = State.PAUSING;
            if (inFlightNormalEvents.isEmpty()) {
                drained.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void enterRecoveryOnly() {
        lock.lock();
        try {
            requireState(State.PAUSING, "enter recovery-only mode");
            if (!inFlightNormalEvents.isEmpty()) {
                throw new IllegalStateException("Cannot enter recovery-only mode before source events drain");
            }
            state = State.RECOVERY_ONLY;
        } finally {
            lock.unlock();
        }
    }

    public void beginResuming() {
        lock.lock();
        try {
            requireState(State.RECOVERY_ONLY, "begin resuming");
            state = State.RESUMING;
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (!inFlightNormalEvents.isEmpty()) {
                if (nanos <= 0L) {
                    return false;
                }
                nanos = drained.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitDrained(long timeoutMillis) throws InterruptedException {
        return awaitDrained(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns whether the event may enter the source queue. A normal event is
     * tracked as in-flight until {@link #release(TapdataEvent)} is called.
     */
    public boolean allow(TapdataEvent event) {
        if (event == null) {
            return false;
        }
        lock.lock();
        try {
            if (isRecoveryTraffic(event)) {
                return state == State.OPEN || state == State.RECOVERY_ONLY;
            }
            if (state != State.OPEN) {
                return false;
            }
            inFlightNormalEvents.add(event);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void release(TapdataEvent event) {
        if (event == null) {
            return;
        }
        lock.lock();
        try {
            if (inFlightNormalEvents.remove(event) && inFlightNormalEvents.isEmpty()) {
                drained.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Restores normal source admission after both successful and exceptional
     * recovery paths. Any accepted source enqueue is still allowed to drain
     * before callers can move into recovery-only mode.
     */
    public void close() {
        open();
    }

    public State getState() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    private boolean isRecoveryTraffic(TapdataEvent event) {
        return event instanceof TapdataDqlRecoveryEvent
                || event instanceof TapdataCountDownLatchEvent
                || TapdataDqlRecoveryEvent.isRecoveryEvent(event.getTapEvent());
    }

    private void requireState(State expected, String operation) {
        if (state != expected) {
            throw new IllegalStateException("Cannot " + operation + " from state " + state);
        }
    }
}
