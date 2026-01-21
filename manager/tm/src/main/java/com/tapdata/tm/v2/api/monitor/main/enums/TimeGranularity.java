package com.tapdata.tm.v2.api.monitor.main.enums;

import lombok.Getter;

@Getter
public enum TimeGranularity {
    SECOND(-1, 1L, null),
    SECOND_FIVE(0, 5L, SECOND),
    MINUTE(1, 60L, SECOND_FIVE),
    HOUR(2, 3600L, MINUTE),
    DAY(3, 86400L, HOUR)

    ;
    final int type;
    final long seconds;
    final TimeGranularity lowerOne;
    TimeGranularity(int t, long seconds, TimeGranularity lowerOne) {
        this.type = t;
        this.seconds = seconds;
        this.lowerOne = lowerOne;
    }

    public long fixTime(long ts) {
        return ts / seconds * seconds;
    }
}
