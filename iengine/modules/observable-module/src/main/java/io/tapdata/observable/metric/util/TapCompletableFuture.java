package io.tapdata.observable.metric.util;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TapCompletableFuture extends CompletableFuture {
    volatile boolean start;
    int maxList;
    Map<Integer, List> mapList = new HashMap<>();
    private LinkedBlockingQueue<List<CompletableFuture>> completableFutureQueue;

    private volatile int indexUse;

    private CompletableFuture<Void> completableFuture;

    private long timeout;

    private boolean pollDataComplete = false;

    private boolean clearAllDataComplete = false;

    private TaskDto task;

    public CompletableFuture<Void> add(CompletableFuture completableFuture) {
        List<CompletableFuture> completableFutureList = mapList.get(indexUse);
        if (completableFutureList.size() >= maxList) {
            while (true) {
                List<CompletableFuture> completableFutures = completableFutureList;
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
            List<CompletableFuture> list = new ArrayList<>();
            mapList.put(index, list);
        }
        getFreeMapList();
        start = true;
        new Thread(() -> {
            while (start) {
                List<CompletableFuture> list;
                try {
                    list = completableFutureQueue.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (CollectionUtils.isNotEmpty(list)) {
                    CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).join();
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
            List<CompletableFuture> pollCompletableFutureList = completableFutureQueue.poll();
            if (CollectionUtils.isEmpty(pollCompletableFutureList)) {
                break;
            }
            if (CollectionUtils.isNotEmpty(pollCompletableFutureList)) {
                CompletableFuture.allOf(pollCompletableFutureList.toArray(new CompletableFuture[0])).join();
                pollCompletableFutureList.clear();
            }
        }
        for (Map.Entry<Integer, List> entry : mapList.entrySet()) {
            if (CollectionUtils.isNotEmpty(entry.getValue())) {
                List<CompletableFuture> completableFutureList = entry.getValue();
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
        for (Map.Entry<Integer, List> entry : mapList.entrySet()) {
            if (CollectionUtils.isEmpty(entry.getValue())) {
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
