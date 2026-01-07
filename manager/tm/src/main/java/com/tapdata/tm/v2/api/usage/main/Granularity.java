package com.tapdata.tm.v2.api.usage.main;

import lombok.Getter;

@Getter
public enum Granularity {
    FIVE_SECOND(5000L),
    MINUTE(60000L),
    HOUR(3600000L);
    
    private final long milliseconds;
    
    Granularity(long milliseconds) {
        this.milliseconds = milliseconds;
    }

}