package io.tapdata.entity.schema.value;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

public class DateTime {
    /**
     * 秒数
     */
    private Long seconds;
    /**
     * 纳秒
     * <p>
     * 毫秒， 微秒， 纳秒， 1000
     */
    //nano int
    private Integer nano;
    /**
     * 时区 GMT+8
     */
    private TimeZone timeZone;

    public DateTime() {
    }

    public DateTime(ZonedDateTime zonedDateTime) {
        this(zonedDateTime.toInstant());
        timeZone = TimeZone.getTimeZone(zonedDateTime.getZone());
    }

    public DateTime(Instant instant) {
        if (instant == null)
            throw new IllegalArgumentException("DateTime constructor instant is null");
        seconds = instant.getEpochSecond();
        nano = instant.getNano();
    }

    public DateTime(Date date) {
        if (date == null)
            throw new IllegalArgumentException("DateTime constructor date is null");
        long time = date.getTime();
        seconds = time / 1000;
        nano = (int) ((time % 1000) * 1000000);
    }

    public DateTime(java.sql.Date date) {
        if (date == null)
            throw new IllegalArgumentException("DateTime constructor date is null");
        long time = date.getTime();
        seconds = time / 1000;
        nano = (int) ((time % 1000) * 1000000);
    }

    public DateTime(java.sql.Time time) {
        if (time == null)
            throw new IllegalArgumentException("DateTime constructor time is null");
        long sqlTime = time.getTime();
        seconds = sqlTime / 1000;
        nano = (int) ((sqlTime % 1000) * 1000000);
    }

    public DateTime(Timestamp timestamp) {
        if (timestamp == null)
            throw new IllegalArgumentException("DateTime constructor timestamp is null");
        long time = timestamp.getTime();
        seconds = time / 1000;
        nano = (int) ((time % 1000) * 1000000);
    }

    public DateTime(Long time, int fraction) {
        if (time == null)
            throw new IllegalArgumentException("DateTime constructor time is null");

        switch (fraction) {
            case 0:
                seconds = time;
                nano = 0;
                break;
            case 3:
                seconds = time / 1000;
                nano = (int) ((time % 1000) * 1000000);
                break;
            case 6:
                seconds = time / 10000000;
                nano = (int) ((time % 1000000) * 1000);
                break;
            case 9:
                seconds = time / 1000000000;
                nano = (int) (time % 1000000000);
                break;
            default:
                throw new IllegalArgumentException("Fraction must be 0, 3, 6, 9");
        }
    }

    /**
     * milliseconds
     * @param time
     */
    public DateTime(Long time) {
        if (time == null)
            throw new IllegalArgumentException("DateTime constructor time is null");

        seconds = time / 1000;
        nano = (int) ((time % 1000) * 1000000);
    }

    public DateTime(LocalDateTime localDateTime) {
        if (localDateTime == null)
            throw new IllegalArgumentException("DateTime constructor localDateTime is null");
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
        seconds = instant.getEpochSecond();
        nano = instant.getNano();
    }

    public Instant toInstant() {
        return Instant.ofEpochSecond(seconds, nano);
    }

    public ZonedDateTime toZonedDateTime() {
        return ZonedDateTime.ofInstant(toInstant(), timeZone.toZoneId());
    }

    public Date toDate() {
        long milliseconds;
        if (seconds != null) {
            milliseconds = seconds * 1000;
            if (nano != null) {
                milliseconds = milliseconds + (nano / 1000 / 1000);
            }
        } else {
            return null;
        }
        return new Date(milliseconds);
    }

    public java.sql.Date toSqlDate() {
        long milliseconds;
        if (seconds != null) {
            milliseconds = seconds * 1000;
            if (nano != null) {
                milliseconds = milliseconds + (nano / 1000 / 1000);
            }
        } else {
            return null;
        }
        return new java.sql.Date(milliseconds);
    }

    public java.sql.Time toTime() {
        long milliseconds;
        if (seconds != null) {
            milliseconds = seconds * 1000;
            if (nano != null) {
                milliseconds = milliseconds + (nano / 1000 / 1000);
            }
        } else {
            return null;
        }
        return new java.sql.Time(milliseconds);
    }

    public Timestamp toTimestamp() {
        long milliseconds;
        if (seconds != null) {
            milliseconds = seconds * 1000;
            if (nano != null) {
                milliseconds = milliseconds + (nano / 1000 / 1000);
            }
        } else {
            return null;
        }
        return new Timestamp(milliseconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTime that = (DateTime) o;
        return Objects.equals(seconds, that.seconds) &&
                Objects.equals(timeZone, that.timeZone) &&
                Objects.equals(nano, that.nano);
    }

    @Override
    public String toString() {
        return "DateTime nano " + nano + " seconds " + seconds + " timeZone " + timeZone;
    }

    public Long getSeconds() {
        return seconds;
    }

    public void setSeconds(Long seconds) {
        this.seconds = seconds;
    }

    public Integer getNano() {
        return nano;
    }

    public void setNano(Integer nano) {
        this.nano = nano;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
