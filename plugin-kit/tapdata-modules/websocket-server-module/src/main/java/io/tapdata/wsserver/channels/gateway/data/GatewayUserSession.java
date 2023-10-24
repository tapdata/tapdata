package io.tapdata.wsserver.channels.gateway.data;

public class GatewayUserSession {
    public static final String TAG = GatewayUserSession.class.getSimpleName();
    /**
     * 玩家id
     */
    private String userId;
    public GatewayUserSession userId(String userId) {
        this.userId = userId;
        return this;
    }
    /**
     * 客户端ip
     */
    private String ip;
    public GatewayUserSession ip(String ip) {
        this.ip = ip;
        return this;
    }

    /**
     * 平台
     */
    private Integer terminal;
    public GatewayUserSession terminal(Integer terminal) {
        this.terminal = terminal;
        return this;
    }

    public Integer getTerminal() {
        return terminal;
    }

    public void setTerminal(Integer terminal) {
        this.terminal = terminal;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
