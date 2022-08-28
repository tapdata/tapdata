package io.tapdata.wsclient.modules.imclient;

import io.tapdata.wsclient.modules.imclient.data.IMResult;

public interface IMMessageResultListener {
    public void onResult(IMResult result);
}
