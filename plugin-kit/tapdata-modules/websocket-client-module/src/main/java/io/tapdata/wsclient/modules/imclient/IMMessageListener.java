package io.tapdata.wsclient.modules.imclient;


import io.tapdata.wsclient.modules.imclient.data.IMMessage;

public interface IMMessageListener {
    public void onMessage(IMMessage message);
}
