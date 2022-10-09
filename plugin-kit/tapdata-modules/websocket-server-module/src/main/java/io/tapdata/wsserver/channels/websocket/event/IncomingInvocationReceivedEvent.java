package io.tapdata.wsserver.channels.websocket.event;

import io.tapdata.modules.api.net.data.IncomingInvocation;

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
