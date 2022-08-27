package io.tapdata.netty.channels.websocket.event;


import io.tapdata.netty.channels.data.IncomingData;

public class IncomingDataReceivedEvent extends NettyEvent<IncomingDataReceivedEvent> {
    private IncomingData incomingData;
    public IncomingDataReceivedEvent incomingData(IncomingData incomingData) {
        this.incomingData = incomingData;
        return this;
    }

    public IncomingData getIncomingData() {
        return incomingData;
    }

    public void setIncomingData(IncomingData incomingData) {
        this.incomingData = incomingData;
    }
}
