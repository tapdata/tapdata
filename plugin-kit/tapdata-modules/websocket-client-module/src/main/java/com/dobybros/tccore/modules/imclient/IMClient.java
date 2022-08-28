package com.dobybros.tccore.modules.imclient;

import com.dobybros.tccore.modules.imclient.data.IMData;
import com.dobybros.tccore.modules.imclient.data.IMMessage;
import com.dobybros.tccore.modules.imclient.data.IMResult;
import com.dobybros.tccore.promise.Promise;

import java.util.concurrent.CompletableFuture;

public interface IMClient {
    public void start();

    public void stop();

    public CompletableFuture<IMResult> sendData(IMData data);

    public CompletableFuture<IMResult> sendMessage(IMMessage message);

    public void registerDataContentType(String contentType, Class<? extends IMData> dataClass);

    public String getPrefix();
}
