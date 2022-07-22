package io.tapdata.dummy.utils;

import io.tapdata.dummy.IBatchConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/10 15:33 Create
 */
public class BatchConsumer<T> implements IBatchConsumer<T> {
    private final int batchSize;
    private final Consumer<List<T>> batchConsumer;
    private List<T> dataLists = new ArrayList<>();
    private final AtomicLong lastTimes = new AtomicLong(System.currentTimeMillis());
    private final Thread timeoutPushTh;

    public BatchConsumer(int batchSize, long timeout, Consumer<List<T>> batchConsumer) {
        this.batchSize = batchSize;
        this.batchConsumer = batchConsumer;
        this.timeoutPushTh = new Thread() {
            @Override
            public void run() {
                long times;
                while (!isInterrupted()) {
                    times = System.currentTimeMillis() - lastTimes.get();
                    if (times > timeout) {
                        if (dataLists.isEmpty()) {
                            lastTimes.set(System.currentTimeMillis());
                        } else {
                            flush();
                        }
                        times = timeout;
                    } else if (times > 0) {
                        times = timeout - times;
                    } else {
                        times = timeout;
                    }

                    try {
                        sleep(times);
                    } catch (InterruptedException ignore) {
                        break;
                    }
                }
            }
        };
        timeoutPushTh.start();
    }

    @Override
    public synchronized void accept(T t) {
        dataLists.add(t);
        if (dataLists.size() >= batchSize) {
            flush();
        }
    }

    private synchronized void flush() {
        batchConsumer.accept(dataLists);
        dataLists = new ArrayList<>();
        lastTimes.set(System.currentTimeMillis());
    }

    @Override
    public void close() throws IOException {
        try {
            // Push data that has not been pushed yet
            flush();
        } finally {
            if (null != timeoutPushTh) {
                timeoutPushTh.interrupt();
            }
        }
    }
}
