package io.tapdata.wsserver.channels.websocket.impl;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.websocket.WebSocketManager;

@Bean
@MainMethod("init")
public class WebSocketProperties {
    @Bean
    WebSocketManager webSocketManager;
    /**
     * 对外端口
     */
    private int publicPort = 443;
    /**
     * 监听端口
     */
    private int port = 8246;

    /**
     * 读取空闲时间，单位：分钟
     */
    private int readIdleTime = 5;

    /**
     * 写入空闲时间，单位：分钟
     */
    private int writeIdleTime = 5;

    /**
     * 读和写空闲时间，单位：分钟
     */
    private int allIdleTime = 5;

    /**
     * 连接队列长度
     */
    private int backlog = 1024;

    /**
     * 是否启用SSL
     */
    private boolean ssl = false;

    public void init() {
        port = CommonUtils.getPropertyInt("tapdata_websocket_port", 8246);
    }
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getReadIdleTime() {
        return readIdleTime;
    }

    public void setReadIdleTime(int readIdleTime) {
        this.readIdleTime = readIdleTime;
    }

    public int getWriteIdleTime() {
        return writeIdleTime;
    }

    public void setWriteIdleTime(int writeIdleTime) {
        this.writeIdleTime = writeIdleTime;
    }

    public int getAllIdleTime() {
        return allIdleTime;
    }

    public void setAllIdleTime(int allIdleTime) {
        this.allIdleTime = allIdleTime;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public int getPublicPort() {
        return publicPort;
    }

    public void setPublicPort(int publicPort) {
        this.publicPort = publicPort;
    }

}
