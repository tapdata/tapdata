package io.tapdata.wsclient.modules.imclient.impls.websocket;

import io.tapdata.wsclient.modules.imclient.impls.PushChannel;

public class ChannelStatus {
    public static final String STATUS_CONNECTED = "connected";
    public static final String STATUS_DISCONNECTED = "disconnected";
    public static final String STATUS_OFFLINEMESSAGECONSUMED = "offlineMessageConsumed";
    public static final String STATUS_KICKED = "kicked";
    public static final String STATUS_BYE = "bye";
    private String status;
    private Integer code;
    private String reason;
    private PushChannel pushChannel;
    public ChannelStatus(PushChannel pushChannel, String status) {
        this(pushChannel, status, null, null);
    }

    public ChannelStatus(PushChannel pushChannel, String status, Integer code) {
        this(pushChannel, status, code, null);
    }
    public ChannelStatus(PushChannel pushChannel, String status, Integer code, String reason) {
        this.pushChannel = pushChannel;
        this.status = status;
        this.code = code;
        this.reason = reason;
    }

    public String toString() {
        return "ChannelStatus " + status + " " + code + " " + reason;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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
