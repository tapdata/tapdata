package io.tapdata.sybase.util;

import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class MultiThreadFactory<T> {
    public static final int DEFAULT_THREAD_COUNT = 3;
    public static final int DEFAULT_EACH_PIECE_SIZE = 50;

    int threadCount;
    int eachPieceSize;

    public MultiThreadFactory(int threadCount, int eachPieceSize) {
        this.eachPieceSize = eachPieceSize;
        this.threadCount = threadCount;
    }

    public MultiThreadFactory(int threadCount) {
        this.threadCount = threadCount;
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
        CopyOnWriteArraySet<List<T>> dataList = new CopyOnWriteArraySet<>(DbKit.splitToPieces(new ArrayList<>(data), eachPieceSize));
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> {
                    try {
                        List<T> eventSpilt;
                        while ((eventSpilt = getOutEventList(dataList)) != null) {
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
            if (EmptyKit.isNotNull(throwable.get())) {
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
        if (EmptyKit.isNotEmpty(tableLists)) {
            List<T> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }
}
