package io.tapdata.wsclient.modules.imclient.impls;


import io.tapdata.wsclient.modules.imclient.data.IMResult;
import com.dobybros.tccore.promise.ResultHandler;

import java.util.concurrent.CompletableFuture;

public class ResultListenerWrapper {

    private String messageId;
    private long time;
    private CompletableFuture<IMResult> resultHandler;

    public ResultListenerWrapper(String messageId, CompletableFuture<IMResult> resultHandler) {
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

    public CompletableFuture<IMResult> getResultHandler() {
        return resultHandler;
    }

    public void setResultHandler(CompletableFuture<IMResult> resultHandler) {
        this.resultHandler = resultHandler;
    }
}
