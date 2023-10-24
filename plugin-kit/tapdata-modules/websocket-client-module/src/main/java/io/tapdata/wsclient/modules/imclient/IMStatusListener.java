package io.tapdata.wsclient.modules.imclient;

public abstract class IMStatusListener {
    public static final int STATUS_INITED = 1;
    public static final int STATUS_CONNECTING = 5;
    public static final int STATUS_CONNECTED = 10;
    public static final int STATUS_DISCONNECTED = 15;
    public static final int STATUS_CLOSED = 20;

    /**
     * handle all kinds of status changes.
     *
     * @param status
     */
    public abstract void onStatusChanged(int status);

    /**
     * handle STATUS_DISCONNECTED
     * given code and message for more logic
     *
     * @param code
     * @param message
     */
    public void disconnected(int code, String message) {}
}
