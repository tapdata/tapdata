package io.tapdata.observable.metric.util;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;

public class TapCompletableFuture extends CompletableFuture {
    volatile boolean start;
    int maxList;
    ConcurrentHashMap<Integer, CopyOnWriteArrayList<CompletableFuture<?>>> mapList = new ConcurrentHashMap<>();
    private LinkedBlockingQueue<CopyOnWriteArrayList<CompletableFuture<?>>> completableFutureQueue;

    private volatile int indexUse;

    private CompletableFuture<Void> completableFuture;

    private long timeout;

    private boolean pollDataComplete = false;

    private boolean clearAllDataComplete = false;

    private TaskDto task;

    public CompletableFuture<Void> add(CompletableFuture completableFuture) {
        CopyOnWriteArrayList<CompletableFuture<?>> completableFutureList = mapList.get(indexUse);
        if (completableFutureList.size() >= maxList) {
            while (true) {
                CopyOnWriteArrayList<CompletableFuture<?>> completableFutures = completableFutureList;
                try {
                    if (completableFutureQueue.offer(completableFutures,50,TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            while (true) {
                if (getFreeMapList() > -1) {
                    break;
                }
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            mapList.get(indexUse).add(completableFuture);
        } else {
            completableFutureList.add(completableFuture);
        }
        return completableFuture;
    }

    public TapCompletableFuture(int queueSize, long timeout, int maxList, TaskDto task) {
        this.maxList = maxList;
        this.timeout = timeout;
        this.task = task;
        completableFuture = CompletableFuture.runAsync(() -> {});
        completableFutureQueue = new LinkedBlockingQueue(queueSize);
        for (int index = 0; index < queueSize; index++) {
            CopyOnWriteArrayList<CompletableFuture<?>> list = new CopyOnWriteArrayList<>();
            mapList.put(index, list);
        }
        getFreeMapList();
        start = true;
        new Thread(() -> {
            while (start) {
                CopyOnWriteArrayList<CompletableFuture<?>> list;
                try {
                    list = completableFutureQueue.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (CollectionUtils.isNotEmpty(list)) {
                    CompletableFuture<?>[] futures = list.stream()
                            .filter(Objects::nonNull)
                            .toArray(CompletableFuture<?>[]::new);

                    CompletableFuture.allOf(futures).join();
                    list.clear();
                }
                if (!start) {
                    pollDataComplete = true;
                }

            }
        },"CompletableFutureQueuePoll-Thread-taskId-"+task.getId()).start();
    }

    public void clearData() {
        while (true) {
            CopyOnWriteArrayList<CompletableFuture<?>> pollCompletableFutureList = completableFutureQueue.poll();
            if (CollectionUtils.isEmpty(pollCompletableFutureList)) {
                break;
            }
            if (CollectionUtils.isNotEmpty(pollCompletableFutureList)) {
                CompletableFuture.allOf(pollCompletableFutureList.toArray(new CompletableFuture[0])).join();
                pollCompletableFutureList.clear();
            }
        }
        for (Map.Entry<Integer, CopyOnWriteArrayList<CompletableFuture<?>>> entry : mapList.entrySet()) {
            if (CollectionUtils.isNotEmpty(entry.getValue())) {
                CopyOnWriteArrayList<CompletableFuture<?>> completableFutureList = entry.getValue();
                CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
                completableFutureList.clear();
            }
        }
        clearAllDataComplete = true;
    }


    public void clearAll() {
        start = false;
        long startTime = System.currentTimeMillis();

        new Thread(() -> {
            clearData();
        },"CompletableFutureClearData-Thread-taskId-"+task.getId()).start();

        while (true) {
            if (System.currentTimeMillis() - startTime > timeout) {
                return;
            }
            if (pollDataComplete && clearAllDataComplete) {
                break;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        completableFuture.cancel(false);

    }

    public int getFreeMapList() {
        for (Map.Entry<Integer, CopyOnWriteArrayList<CompletableFuture<?>>> entry : mapList.entrySet()) {
            if (CollectionUtils.isEmpty(entry.getValue()) ||  entry.getValue().size() < maxList) {
                indexUse = entry.getKey();
                return indexUse;
            }
        }
        indexUse = -1;
        return indexUse;

    }

    public CompletableFuture<Void> getCompletableFuture() {
        return completableFuture;
    }

}
