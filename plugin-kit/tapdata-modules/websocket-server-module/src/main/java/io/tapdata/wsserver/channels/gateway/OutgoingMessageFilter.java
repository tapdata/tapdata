package io.tapdata.wsserver.channels.gateway;


import io.tapdata.wsserver.channels.data.OutgoingMessage;

public interface OutgoingMessageFilter {
    void received(OutgoingMessage outgoingMessage);
}