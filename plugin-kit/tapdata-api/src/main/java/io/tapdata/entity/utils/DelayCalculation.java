package io.tapdata.entity.utils;

import io.tapdata.entity.logger.TapLogger;

import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/8 21:11 Create
 */
public class DelayCalculation {
    private final static String TAG = DelayCalculation.class.getSimpleName();

    private long counts = 0;
    private long minDelay = Long.MAX_VALUE;
    private long maxDelay = Long.MIN_VALUE;
    private long firstCreate;
    private long firstWrite;
    private long lastCreate;
    private long lastWrite;

    private Consumer<Long> changeConsumer = createTimes -> {
        counts++;
        lastCreate = createTimes;
        lastWrite = System.currentTimeMillis();
        minDelay = Math.min(minDelay, lastWrite - lastCreate);
        maxDelay = Math.max(maxDelay, lastWrite - lastCreate);
    };

    private Consumer<Long> logConsumer = firstTime -> {
        firstCreate = firstTime;
        firstWrite = System.currentTimeMillis();

        changeConsumer.accept(firstTime);
        logConsumer = changeConsumer;
    };

    public DelayCalculation(long logInterval) {
        if (logInterval > 0) {
            DelayCalculation _this = this;

            Consumer<DelayCalculation> printConsumer = new Consumer<DelayCalculation>() {
                private long lastTimes;
                @Override
                public void accept(DelayCalculation ins) {
                    if (System.currentTimeMillis() - lastTimes > logInterval) {
                        lastTimes = System.currentTimeMillis();
                    }
                }
            };

            changeConsumer = createTimes -> {
                counts++;
                lastCreate = createTimes;
                lastWrite = System.currentTimeMillis();
                minDelay = Math.min(minDelay, lastWrite - lastCreate);
                maxDelay = Math.max(maxDelay, lastWrite - lastCreate);
                printConsumer.accept(_this);
            };
        }
    }

    public boolean hasData() {
        return counts > 0;
    }

    public synchronized void log(long createTimes) {
        logConsumer.accept(createTimes);
    }

    @Override
    public String toString() {
        long useTimes = lastWrite - firstCreate;
        return String.format("first %d %dms, last %d %dms, counts: %d/%dms, min: %s, max: %s, QPS: %d/s"
                , firstCreate, (firstWrite - firstCreate)
                , lastCreate, (lastWrite - lastCreate)
                , counts, useTimes
                , minDelay, maxDelay
                , (useTimes > 1000 ? (counts / (useTimes / 1000)) : counts));
    }
}
