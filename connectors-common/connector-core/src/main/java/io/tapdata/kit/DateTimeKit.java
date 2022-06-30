package io.tapdata.kit;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class DateTimeKit {

    public static Timestamp convertRedoContentTimestamp(Timestamp timestamp, ZoneId sysTimezone) {
        if (timestamp == null || sysTimezone == null) {
            return timestamp;
        }
        // jdbc会获取jvm的默认时区，作为timestamp的时区，需要去除
        LocalDateTime localDateTime = timestamp.toLocalDateTime();

        // 根据oracle的system time zone，将timestamp进行时区转换
        timestamp = Timestamp.from(localDateTime.atZone(sysTimezone).toInstant());

        return timestamp;
    }
}
