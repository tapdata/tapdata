package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.modules.api.net.data.Result;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

public class ResultListenerWrapper {

    private String messageId;
    private long time;
    private CompletableFuture<Result> resultHandler;
    private ScheduledFuture timeoutFuture;

    public ResultListenerWrapper(String messageId, CompletableFuture<Result> resultHandler, ScheduledFuture timeoutFuture) {
        this.messageId = messageId;
        this.resultHandler = resultHandler;
        this.timeoutFuture = timeoutFuture;

        time = System.currentTimeMillis();
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
}
