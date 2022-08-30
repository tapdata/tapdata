package io.tapdata.kit;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class DateTimeKit {

    public static Timestamp convertRedoContentTimestamp(Timestamp timestamp, ZoneId sysTimezone) {
        if (timestamp == null || sysTimezone == null) {
            return timestamp;
        }
        // JDBC will get the default time zone of the JVM as the time zone of timestamp, which needs to be removed
        LocalDateTime localDateTime = timestamp.toLocalDateTime();

        // Convert timestamp to time zone according to Oracle's system time zone
        timestamp = Timestamp.from(localDateTime.atZone(sysTimezone).toInstant());

        return timestamp;
    }
}
