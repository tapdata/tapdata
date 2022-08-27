package io.tapdata.netty.channels.websocket.event;


import io.tapdata.netty.channels.data.IncomingMessage;

public class IncomingMessageReceivedEvent extends NettyEvent<IncomingMessageReceivedEvent> {
    private IncomingMessage incomingMessage;
    public IncomingMessageReceivedEvent incomingMessage(IncomingMessage incomingMessage) {
        this.incomingMessage = incomingMessage;
        return this;
    }

    public IncomingMessage getIncomingMessage() {
        return incomingMessage;
    }

    public void setIncomingMessage(IncomingMessage incomingMessage) {
        this.incomingMessage = incomingMessage;
    }
}
