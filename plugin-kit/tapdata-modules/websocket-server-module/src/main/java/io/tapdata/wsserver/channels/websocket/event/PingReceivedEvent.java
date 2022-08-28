package io.tapdata.wsserver.channels.websocket.event;


import io.tapdata.wsserver.channels.data.Ping;

public class PingReceivedEvent extends NettyEvent<PingReceivedEvent> {
    private Ping ping;
    public PingReceivedEvent ping(Ping ping) {
        this.ping = ping;
        return this;
    }

    public Ping getPing() {
        return ping;
    }

    public void setPing(Ping ping) {
        this.ping = ping;
    }
}
