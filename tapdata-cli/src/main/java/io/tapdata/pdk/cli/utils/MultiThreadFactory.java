package io.tapdata.pdk.cli.utils;

import io.tapdata.pdk.cli.utils.split.SplitStage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author Gavin'Xiao
 * https://github.com/11000100111010101100111
 * */
public class MultiThreadFactory<T> implements SplitStage<T> {
    public static final int DEFAULT_THREAD_COUNT = 3;
    SplitStage<T> splitStage;

    int threadCount;

    public MultiThreadFactory(int threadCount) {
        this.threadCount = Math.max(threadCount, 1);
    }

    public MultiThreadFactory<T> setSplitStage(SplitStage<T> splitStage) {
        if (null != splitStage) {
            this.splitStage = splitStage;
        }
        return this;
    }

    public MultiThreadFactory() {
        this.threadCount = DEFAULT_THREAD_COUNT;
    }

    public void handel(List<T> data, ThreadConsumer<T> consumer) {
        handel(threadCount, data, consumer);
    }

    private void handel(int threadCount, List<T> data, ThreadConsumer<T> consumer) {
        if (null == data || data.isEmpty()) return;
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final AtomicInteger local = new AtomicInteger(0);
        final List<T> execData = data.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (execData.isEmpty()) return;
        try {
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> {
                    try {
                        T eventSpilt;
                        while ((eventSpilt = getOutEvent(execData, local)) != null) {
                            consumer.accept(eventSpilt);
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (null != throwable.get()) {
                throw new RuntimeException(throwable.get());
            }
        } finally {
            executorService.shutdown();
        }
    }

    public interface ThreadConsumer<T> {
        public void accept(T eventSpilt);
    }

    private T getOutEvent(List<T> tableLists, final AtomicInteger local) {
        if (null != tableLists) {
            T value;
            synchronized (local) {
                value = tableLists.get(local.get());
                local.incrementAndGet();
            }
            return value;
        }
        return null;
    }

    public List<List<T>> splitToPieces(List<T> data, int eachPieceSize) {
        if (null == data || data.isEmpty()) {
            return new ArrayList<>();
        }
        if (eachPieceSize <= 0) {
            throw new IllegalArgumentException("Param Error");
        }
        List<List<T>> result = new ArrayList<>();
        for (int index = 0; index < data.size(); index += eachPieceSize) {
            result.add(data.stream().skip(index).limit(eachPieceSize).collect(Collectors.toList()));
        }
        return result;
    }
}
