package io.tapdata.netty.channels.websocket.event;

import io.tapdata.netty.channels.data.IncomingInvocation;

public class IncomingInvocationReceivedEvent extends NettyEvent<IncomingInvocationReceivedEvent> {
    private IncomingInvocation incomingInvocation;
    public IncomingInvocationReceivedEvent incomingInvocation(IncomingInvocation incomingInvocation) {
        this.incomingInvocation = incomingInvocation;
        return this;
    }

    public IncomingInvocation getIncomingInvocation() {
        return incomingInvocation;
    }

    public void setIncomingInvocation(IncomingInvocation incomingInvocation) {
        this.incomingInvocation = incomingInvocation;
    }
}
