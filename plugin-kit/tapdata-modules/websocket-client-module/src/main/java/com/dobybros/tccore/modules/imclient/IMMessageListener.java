package com.dobybros.tccore.modules.imclient;


import com.dobybros.tccore.modules.imclient.data.IMMessage;

public interface IMMessageListener {
    public void onMessage(IMMessage message);
}
