package io.tapdata.flow.engine.V2.util;

import io.tapdata.observable.logging.ObsLogger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class DelayHandler {


    private static final long MAX_DELAY = TimeUnit.MILLISECONDS.toMicros(100);
    private static final long MIN_DELAY = TimeUnit.MICROSECONDS.toMicros(0);
    private static final long UNIT = TimeUnit.MICROSECONDS.toMicros(10);
    private static final long WARN_DELAY = TimeUnit.MILLISECONDS.toMicros(10);
    private final ObsLogger obsLogger;

    private final String TAG;
    private final AtomicLong succeeded = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);

    private final AtomicBoolean end = new AtomicBoolean(false);

    public DelayHandler(ObsLogger obsLogger, String tag) {
        this.obsLogger = obsLogger;
        TAG = tag;
    }

    public boolean process(Supplier<Boolean> supplier) {
        boolean processResult = supplier.get();
        try {
            if (processResult) {
                succeeded.incrementAndGet();
                if(end.get()){
                    obsLogger.info("[{}] Successor node processing speed recovery", TAG);
                    end.set(false);
                }
            } else {
                failed.incrementAndGet();
                sleep();
            }
        } catch (Throwable ignore) {
            obsLogger.warn("Delay push failed, ignore processing", ignore);
        }
        return processResult;

    }

    private void sleep() throws InterruptedException {
        long relay = delay();
        if (relay > MIN_DELAY) {
            if (obsLogger.isDebugEnabled()) {
                obsLogger.debug("[{}} Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
            }
            if (relay > WARN_DELAY && !end.get()) {
                obsLogger.warn("[{}] Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
                end.set(true);
            }
            TimeUnit.MICROSECONDS.sleep(relay);
        }
    }

    private long delay() {
        long relay = failed.get() * UNIT - succeeded.get() * UNIT;
        if (relay < MIN_DELAY) {
            relay = 0;
        }
        if (relay > MAX_DELAY) {
            relay = MAX_DELAY;
        }
        return relay;
    }

}
