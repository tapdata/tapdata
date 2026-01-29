package com.tapdata.tm.apiServer.enums;

import lombok.Getter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Getter
public enum TimeGranularity {
    SECOND(-1, 1L, null, 0L),
    SECOND_FIVE(0, 5L, SECOND, 5 * 60L),
    MINUTE(1, 60L, SECOND_FIVE, 60L * 60L),
    HOUR(2, 3600L, MINUTE, 0L),
    DAY(3, 86400L, HOUR, 0L)

    ;
    final int type;
    final long seconds;
    final long supplement;
    final TimeGranularity lowerOne;
    TimeGranularity(int t, long seconds, TimeGranularity lowerOne, long supplement) {
        this.type = t;
        this.seconds = seconds;
        this.lowerOne = lowerOne;
        this.supplement = supplement;
    }

    public long fixTime(long ts) {
        if (this == DAY) {
            return toStartOfDay(ts);
        }
        return ts / seconds * seconds;
    }

    public static long toStartOfDay(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp * 1000L);
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
        LocalDate localDate = zonedDateTime.toLocalDate();
        LocalDateTime startOfDay = localDate.atStartOfDay();
        return startOfDay.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000L;
    }
}
