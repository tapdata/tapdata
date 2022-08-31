package io.tapdata.wsclient.modules.imclient;

import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;

import java.util.concurrent.CompletableFuture;

public interface IMClient {
    void start();

    void stop();

    CompletableFuture<Result> sendData(IncomingData data);

    CompletableFuture<Result> sendMessage(IncomingMessage message);

    void registerDataContentType(String contentType, Class<? extends Data> dataClass);

    String getPrefix();
}
