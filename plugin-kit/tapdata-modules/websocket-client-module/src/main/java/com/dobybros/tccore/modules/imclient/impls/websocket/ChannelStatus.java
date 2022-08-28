package com.dobybros.tccore.modules.imclient.impls.websocket;

import com.dobybros.tccore.modules.imclient.impls.PushChannel;

public class ChannelStatus {
    public static final String STATUS_CONNECTED = "connected";
    public static final String STATUS_DISCONNECTED = "disconnected";
    public static final String STATUS_OFFLINEMESSAGECONSUMED = "offlineMessageConsumed";
    public static final String STATUS_KICKED = "kicked";
    public static final String STATUS_BYE = "bye";
    private String type;
    private Integer code;
    private String reason;
    private PushChannel pushChannel;
    public ChannelStatus(PushChannel pushChannel, String type) {
        this(pushChannel, type, null, null);
    }

    public ChannelStatus(PushChannel pushChannel, String type, Integer code) {
        this(pushChannel, type, code, null);
    }
    public ChannelStatus(PushChannel pushChannel, String type, Integer code, String reason) {
        this.pushChannel = pushChannel;
        this.type = type;
        this.code = code;
        this.reason = reason;
    }

    public String toString() {
        return "ChannelStatus " + type + " " + code + " " + reason;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public PushChannel getPushChannel() {
        return pushChannel;
    }

    public void setPushChannel(PushChannel pushChannel) {
        this.pushChannel = pushChannel;
    }
}
