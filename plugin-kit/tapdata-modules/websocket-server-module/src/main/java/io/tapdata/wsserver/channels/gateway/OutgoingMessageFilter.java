package io.tapdata.wsserver.channels.gateway;


import io.tapdata.modules.api.net.data.OutgoingMessage;

public interface OutgoingMessageFilter {
    void received(OutgoingMessage outgoingMessage);
}