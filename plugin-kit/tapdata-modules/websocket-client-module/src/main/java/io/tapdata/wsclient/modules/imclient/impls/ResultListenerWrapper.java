package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.modules.api.net.data.Result;

import java.util.concurrent.CompletableFuture;

public class ResultListenerWrapper {

    private String messageId;
    private long time;
    private CompletableFuture<Result> resultHandler;

    public ResultListenerWrapper(String messageId, CompletableFuture<Result> resultHandler) {
        this.messageId = messageId;
        this.resultHandler = resultHandler;

        time = System.currentTimeMillis();
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
