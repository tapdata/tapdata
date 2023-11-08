package io.tapdata.mongodb.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/6 19:03 Create
 */
public abstract class IntervalReport<T, E extends Throwable> {
    private final int interval;
    private final AtomicLong lastSupportTimes = new AtomicLong(System.currentTimeMillis());

    protected IntervalReport(int interval) {
        this.interval = interval;
    }

    public void summit(T value) throws E {
        if (System.currentTimeMillis() - lastSupportTimes.get() > interval) {
            report(value);
            lastSupportTimes.set(System.currentTimeMillis());
        }
    }

    public void clear() {
        lastSupportTimes.set(System.currentTimeMillis());
    }

    protected abstract void report(T lastValue) throws E;
}
