package io.tapdata.wsserver.channels.gateway.data;

import java.util.Date;

public class UserChannel {
    private String userId;
    public UserChannel userId(String userId) {
        this.userId = userId;
        return this;
    }
    /**
     * 可以授权访问的服务+类名+方法名的正则表达式
     */
    private String authorisedExpression;
    public UserChannel authorisedExpression(String authorisedExpression) {
        this.authorisedExpression = authorisedExpression;
        return this;
    }
    private String deviceToken;
    public UserChannel deviceToken(String deviceToken) {
        this.deviceToken = deviceToken;
        return this;
    }

    private Integer terminal;
    public UserChannel terminal(Integer terminal) {
        this.terminal = terminal;
        return this;
    }

    private Long createTime, updateTime;
    public UserChannel createTime(Long createTime) {
        this.createTime = createTime;
        return this;
    }
    public UserChannel updateTime(Long updateTime) {
        this.updateTime = updateTime;
        return this;
    }

    private String ip;
    public UserChannel ip(String ip) {
        this.ip = ip;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getAuthorisedExpression() {
        return authorisedExpression;
    }

    public void setAuthorisedExpression(String authorisedExpression) {
        this.authorisedExpression = authorisedExpression;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDeviceToken() {
        return deviceToken;
    }

    public void setDeviceToken(String deviceToken) {
        this.deviceToken = deviceToken;
    }

    public Integer getTerminal() {
        return terminal;
    }

    public void setTerminal(Integer terminal) {
        this.terminal = terminal;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return UserChannel.class.getSimpleName() + ": userId " + userId + " authorisedExpression " + authorisedExpression + " deviceToken " + deviceToken + " terminal " + terminal + " createTime " + (createTime != null ? new Date(createTime) : null) + " updateTime " + (updateTime != null ? new Date(updateTime) : null) + " ip " + ip;
    }
}
