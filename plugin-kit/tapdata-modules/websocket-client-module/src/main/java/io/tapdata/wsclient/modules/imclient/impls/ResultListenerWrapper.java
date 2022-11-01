package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.pdk.core.implementation.BeanAnnotationHandler;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ResultListenerWrapper implements Comparable<ResultListenerWrapper>, MemoryFetcher {
    private String messageId;
    private long time;
    private CompletableFuture<Result> resultHandler;
    private ScheduledFuture timeoutFuture;
    private long counter;
    private Data data;

    public ResultListenerWrapper(IncomingMessage incomingMessage, CompletableFuture<Result> resultHandler, ScheduledFuture timeoutFuture, long counter) {
        this.data = incomingMessage;
        this.messageId = incomingMessage.getId();
        this.resultHandler = resultHandler;
        this.timeoutFuture = timeoutFuture;
        this.counter = counter;

        time = System.currentTimeMillis();
    }
    public ResultListenerWrapper(IncomingData incomingData, CompletableFuture<Result> resultHandler, ScheduledFuture timeoutFuture, long counter) {
        this.data = incomingData;
        this.messageId = incomingData.getId();
        this.resultHandler = resultHandler;
        this.timeoutFuture = timeoutFuture;
        this.counter = counter;

        time = System.currentTimeMillis();
    }
    @Override
    public int compareTo(ResultListenerWrapper resultListenerWrapper) {
//        if(order == interceptorClassHolder.order)
//            return 0;
        return counter > resultListenerWrapper.counter ? 1 : -1;
    }
    public void complete(ConcurrentHashMap<String, ResultListenerWrapper> resultMap, Result result) {
        ResultListenerWrapper wrapper = resultMap.remove(messageId);
        if(wrapper != null) {
            timeoutFuture.cancel(true);
            if(!resultHandler.isDone())
                resultHandler.complete(result);
        }
    }

    public void completeExceptionally(ConcurrentHashMap<String, ResultListenerWrapper> resultMap, Throwable throwable) {
        ResultListenerWrapper wrapper = resultMap.remove(messageId);
        if(wrapper != null) {
            timeoutFuture.cancel(true);
            if(!resultHandler.isDone())
                resultHandler.completeExceptionally(throwable);
        }
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public CompletableFuture<Result> getResultHandler() {
        return resultHandler;
    }

    public void setResultHandler(CompletableFuture<Result> resultHandler) {
        this.resultHandler = resultHandler;
    }

    public ScheduledFuture getTimeoutFuture() {
        return timeoutFuture;
    }

    public void setTimeoutFuture(ScheduledFuture timeoutFuture) {
        this.timeoutFuture = timeoutFuture;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)
                .kv("messageId", messageId)
                .kv("time", time)
                .kv("timeoutFuture", timeoutFuture != null ? timeoutFuture.getDelay(TimeUnit.MILLISECONDS) : null)
                ;
    }
}
