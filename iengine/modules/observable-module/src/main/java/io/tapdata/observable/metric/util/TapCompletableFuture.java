package io.tapdata.observable.metric.util;

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
    int maxList = 1000;
    Map<Integer, List> mapList = new HashMap<>();
    private LinkedBlockingQueue<List<CompletableFuture>> completableFutureQueue;

    private volatile int indexUse;

    private CompletableFuture<Void> completableFuture;

    private long timeout = 6000L;

    private boolean pollDataComplete = false;

    private boolean clearAllDataComplete = false;

    public CompletableFuture<Void> add(CompletableFuture completableFuture) {
        List<CompletableFuture> completableFutureList = mapList.get(indexUse);
        if (completableFutureList.size() >= maxList) {
            while (true) {
                List<CompletableFuture> completableFutures = completableFutureList;
                if (completableFutureQueue.offer(completableFutures)) {
                    break;
                }
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
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
                    break;
                }
            }
            mapList.get(indexUse).add(completableFuture);
        } else {
            completableFutureList.add(completableFuture);
        }
        return completableFuture;
    }

    public TapCompletableFuture(int queueSize) {
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
                    break;
                }
                if (CollectionUtils.isNotEmpty(list)) {
                    CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).join();
                    list.clear();
                }
                if (!start) {
                    pollDataComplete = true;
                }

            }
        }).start();
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
        List<CompletableFuture> completableFutureList = mapList.get(indexUse);
        if(CollectionUtils.isNotEmpty(completableFutureList)) {
            CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
            completableFutureList.clear();
        }
        clearAllDataComplete = true;
    }


    public void clearAll() {
        start = false;
        long start = System.currentTimeMillis();

        new Thread(() -> {
            clearData();
        }).start();

        while (true) {
            if (System.currentTimeMillis() - start > timeout) {
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
        for (Integer key : mapList.keySet()) {
            if (CollectionUtils.isEmpty(mapList.get(key))) {
                indexUse = key;
                return key;
            }
        }
        return indexUse = -1;
    }

    public CompletableFuture<Void> getCompletableFuture() {
        return completableFuture;
    }

}
