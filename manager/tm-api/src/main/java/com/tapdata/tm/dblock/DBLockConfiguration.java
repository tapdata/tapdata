package com.tapdata.tm.dblock;

import lombok.Data;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Data
@Configuration
@ConfigurationProperties(prefix = "db-lock")
public class DBLockConfiguration {
    private String owner;
    private long expireSeconds = TimeUnit.MINUTES.toSeconds(1);
    private long heartbeatSeconds = TimeUnit.SECONDS.toSeconds(30);

    public synchronized void setOwner(String owner) {
        this.owner = owner;
    }

    public synchronized String getOwner() {
        if (null == owner || owner.isBlank()) {
            owner = SystemUtils.getHostName();
            if (null == owner || owner.isBlank()) {
                owner = UUID.randomUUID().toString();
            }
            owner = String.format("%s-%d", owner, System.currentTimeMillis());
        }
        return owner;
    }
}
