package io.tapdata.netty.channels.gateway;


import io.tapdata.netty.channels.data.OutgoingMessage;

public interface OutgoingMessageFilter {
    void received(OutgoingMessage outgoingMessage);
}