package io.tapdata.pdk.cli.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MultiThreadFactory<T> {
    public static final int DEFAULT_THREAD_COUNT = 3;
    public static final int DEFAULT_EACH_PIECE_SIZE = 50;

    int threadCount;
    int eachPieceSize;

    public MultiThreadFactory(int threadCount, int eachPieceSize) {
        this.eachPieceSize = eachPieceSize;
        this.threadCount = Math.max(threadCount, 1);
    }

    public MultiThreadFactory(int threadCount) {
        this.threadCount = Math.max(threadCount, 1);
        this.eachPieceSize = DEFAULT_EACH_PIECE_SIZE;
    }

    public MultiThreadFactory() {
        this.threadCount = DEFAULT_THREAD_COUNT;
        this.eachPieceSize = DEFAULT_EACH_PIECE_SIZE;
    }

    public void handel(List<T> data, ThreadConsumer<T> consumer) {
        handel(threadCount, eachPieceSize, data, consumer);
    }

    private void handel(int threadCount, int eachPieceSize, List<T> data, ThreadConsumer<T> consumer) {
        CopyOnWriteArraySet<List<T>> dataList = new CopyOnWriteArraySet<>(splitToPieces(new ArrayList<>(data), eachPieceSize));
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> {
                    try {
                        List<T> eventSpilt;
                        while ((eventSpilt = getOutEventList(dataList)) != null && !eventSpilt.isEmpty()) {
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
        public void accept(List<T> eventSpilt);
    }

    private synchronized List<T> getOutEventList(CopyOnWriteArraySet<List<T>> tableLists) {
        if (null != tableLists) {
            List<T> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }

    public static <T> List<List<T>> splitToPieces(List<T> data, int eachPieceSize) {
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
