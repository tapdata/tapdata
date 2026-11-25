package io.tapdata.observable.metric.throttle;

/**
 * Clock abstraction enabling deterministic virtual-clock testing alongside production monotonic timing.
 */
public interface MonotonicClock {

    long nanoTime();

    long currentTimeMillis();

    MonotonicClock SYSTEM = new MonotonicClock() {
        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    };
}
