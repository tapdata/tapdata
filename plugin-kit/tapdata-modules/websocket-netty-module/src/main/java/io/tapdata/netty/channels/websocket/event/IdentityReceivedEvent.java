package io.tapdata.netty.channels.websocket.event;


import io.tapdata.netty.channels.data.Identity;

public class IdentityReceivedEvent extends NettyEvent<IdentityReceivedEvent> {
    private Identity identity;
    public IdentityReceivedEvent identity(Identity identity) {
        this.identity = identity;
        return this;
    }

    public Identity getIdentity() {
        return identity;
    }

    public void setIdentity(Identity identity) {
        this.identity = identity;
    }
}
