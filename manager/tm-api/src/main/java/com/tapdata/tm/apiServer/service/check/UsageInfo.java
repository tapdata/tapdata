package com.tapdata.tm.apiServer.service.check;

import lombok.Data;

@Data
public final class UsageInfo {
    String serverId;
    String workerId;
    Number usage;
    Long time;
    String userId;

    public UsageInfo serverId(String serverId) {
        this.serverId = serverId;
        return this;
    }

    public UsageInfo workerId(String workerId) {
        this.serverId = workerId;
        return this;
    }

    public UsageInfo usage(Number usage) {
        this.usage = usage;
        return this;
    }

    public UsageInfo time(Long time) {
        this.time = time;
        return this;
    }

    public UsageInfo userId(String userId) {
        this.userId = userId;
        return this;
    }
}