package io.tapdata.wsserver.channels.gateway;

import io.tapdata.modules.api.net.data.*;
import io.tapdata.wsserver.channels.gateway.data.UserChannel;

public class UserAction {
    private GatewaySessionHandler handler;
    public UserAction handler(GatewaySessionHandler handler) {
        this.handler = handler;
        return this;
    }
    private String userId;
    public UserAction userId(String userId) {
        this.userId = userId;
        return this;
    }

    public static final int ACTION_SESSION_CREATED = 10;
    public static final int ACTION_USER_CONNECTED = 20;//channel通过验证，正式连接
    public static final int ACTION_USER_DISCONNECTED = 30;
    public static final int ACTION_SESSION_DESTROYED = 40;

    public static final int ACTION_USER_DATA = 105;
    public static final int ACTION_USER_MESSAGE = 100;
    public static final int ACTION_USER_INVOCATION = 110;
    public static final int ACTION_USER_REQUEST = 115;
    public static final int ACTION_USER_OUTGOING_MESSAGE = 120;
    public static final int ACTION_USER_OUTGOING_DATA = 130;

    public static final int ACTION_USER_CLOSURE = 1000;
    private int action;
    public UserAction action(int action) {
        this.action = action;
        return this;
    }
    private UserChannel userChannel;
    public UserAction userChannel(UserChannel userChannel) {
        this.userChannel = userChannel;
        return this;
    }

    private IncomingData incomingData;
    public UserAction incomingData(IncomingData incomingData) {
        this.incomingData = incomingData;
        return this;
    }

    private IncomingMessage incomingMessage;
    public UserAction incomingMessage(IncomingMessage incomingMessage) {
        this.incomingMessage = incomingMessage;
        return this;
    }

    private IncomingInvocation incomingInvocation;
    public UserAction incomingInvocation(IncomingInvocation incomingInvocation) {
        this.incomingInvocation = incomingInvocation;
        return this;
    }

    private IncomingRequest incomingRequest;
    public UserAction incomingRequest(IncomingRequest incomingRequest) {
        this.incomingRequest = incomingRequest;
        return this;
    }

    private OutgoingMessage outgoingMessage;
    public UserAction outgoingMessage(OutgoingMessage outgoingMessage) {
        this.outgoingMessage = outgoingMessage;
        return this;
    }

    private OutgoingData outgoingData;
    public UserAction outgoingData(OutgoingData outgoingData) {
        this.outgoingData = outgoingData;
        return this;
    }
//
    private Runnable closure;
    public UserAction closure(Runnable closure) {
        this.closure = closure;
        return this;
    }

    public GatewaySessionHandler getHandler() {
        return handler;
    }

    public void setHandler(GatewaySessionHandler handler) {
        this.handler = handler;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public UserChannel getUserChannel() {
        return userChannel;
    }

    public void setUserChannel(UserChannel userChannel) {
        this.userChannel = userChannel;
    }

    public IncomingData getIncomingData() {
        return incomingData;
    }

    public void setIncomingData(IncomingData incomingData) {
        this.incomingData = incomingData;
    }

    public IncomingMessage getIncomingMessage() {
        return incomingMessage;
    }

    public void setIncomingMessage(IncomingMessage incomingMessage) {
        this.incomingMessage = incomingMessage;
    }

    public IncomingInvocation getIncomingInvocation() {
        return incomingInvocation;
    }

    public void setIncomingInvocation(IncomingInvocation incomingInvocation) {
        this.incomingInvocation = incomingInvocation;
    }

    public IncomingRequest getIncomingRequest() {
        return incomingRequest;
    }

    public void setIncomingRequest(IncomingRequest incomingRequest) {
        this.incomingRequest = incomingRequest;
    }

    public OutgoingMessage getOutgoingMessage() {
        return outgoingMessage;
    }

    public void setOutgoingMessage(OutgoingMessage outgoingMessage) {
        this.outgoingMessage = outgoingMessage;
    }

    public Runnable getClosure() {
        return closure;
    }

    public void setClosure(Runnable closure) {
        this.closure = closure;
    }

    public OutgoingData getOutgoingData() {
        return outgoingData;
    }

    public void setOutgoingData(OutgoingData outgoingData) {
        this.outgoingData = outgoingData;
    }
}
