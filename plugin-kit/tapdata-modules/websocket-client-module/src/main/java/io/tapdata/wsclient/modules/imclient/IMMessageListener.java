package io.tapdata.wsclient.modules.imclient;

import io.tapdata.modules.api.net.data.Data;

public interface IMMessageListener {
    void onMessage(Data message);
}
