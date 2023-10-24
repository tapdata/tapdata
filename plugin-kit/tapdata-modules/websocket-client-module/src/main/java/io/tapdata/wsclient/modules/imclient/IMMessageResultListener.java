package io.tapdata.wsclient.modules.imclient;

import io.tapdata.modules.api.net.data.Result;

public interface IMMessageResultListener {
    void onResult(Result result);
}
