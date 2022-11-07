/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tapdata.common.cdc;

import io.tapdata.kit.EmptyKit;
import io.tapdata.util.DateUtil;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeColumnHandler {

    public static final String DEFAULT_LOCAL_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]";
    public static final String DEFAULT_ZONED_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS] VV";
    public static final String DEFAULT_DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss";

    private final Pattern toDatePattern = Pattern.compile("TO_DATE\\('(.*)',.*");
    // If a date is set into a timestamp column (or a date field is widened to a timestamp,
    // a timestamp ending with "." is returned (like 2016-04-15 00:00:00.), so we should also ignore the trailing ".".
    private final Pattern toTimestampPattern = Pattern.compile("TO_TIMESTAMP\\('(.*[^.]).*'");
    // TIMESTAMP WITH LOCAL TIME ZONE contains a "." at the end just like timestamp, so ignore that.
    private final Pattern toTimeStampTzPatternLocalTz = Pattern.compile("TO_TIMESTAMP_TZ\\('(.*[^.]).*'");
    private final Pattern toTimeStampTzPatternTz = Pattern.compile("TO_TIMESTAMP_TZ\\('(.*).*'");
    static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    static final DateTimeFormatter TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter LOCAL_DT_FORMATTER =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter();
    private static final DateTimeFormatter ZONED_DT_FORMATTER =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .appendLiteral(' ')
                    .appendZoneId()
                    .toFormatter();
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String TIMESTAMP = "TIMESTAMP";

    public final DateTimeFormatter dateFormatter;
    private final DateTimeFormatter localDtFormatter;
    private final DateTimeFormatter zonedDtFormatter;

    private final ZoneId zoneId;

    public DateTimeColumnHandler(ZoneId zoneId) {
        this(zoneId, DEFAULT_DATETIME_FORMAT, DEFAULT_LOCAL_DATETIME_FORMAT, DEFAULT_ZONED_DATETIME_FORMAT);
    }

    public DateTimeColumnHandler(
            ZoneId zoneId,
            String dateFormat,
            String localDateTimeFormat,
            String zonedDatetimeFormat
    ) {
        this.zoneId = zoneId;
        dateFormatter =
                new DateTimeFormatterBuilder()
                        .parseLenient()
                        .appendPattern(dateFormat)
                        .toFormatter();
        localDtFormatter =
                new DateTimeFormatterBuilder()
                        .parseLenient()
                        .appendPattern(localDateTimeFormat)
                        .toFormatter();

        zonedDtFormatter =
                new DateTimeFormatterBuilder()
                        .parseLenient()
                        .appendPattern(zonedDatetimeFormat)
                        .toFormatter();
    }

    /**
     * This method returns an {@linkplain } that represents a DATE, TIME or TIMESTAMP. It is possible for user to upgrade
     * a field from DATE to TIMESTAMP, and if we read the table schema on startup after this upgrade, we would assume the field
     * should be returned as DATETIME field. But it is possible that the first change we read was made before the upgrade from
     * DATE to TIMESTAMP. So we check whether the returned SQL has TO_TIMESTAMP - if it does we return it as DATETIME, else we
     * return it as DATE.
     */
    public Object getDateTimeStampField(
            String columnValue,
            String actualType
    ) {
        if (columnValue == null) {
            return null;
        } else {

            Optional<String> ts = matchDateTimeString(toTimestampPattern.matcher(columnValue));
            if (ts.isPresent()) {
                return Timestamp.valueOf(ts.get());
            }
            // We did not find TO_TIMESTAMP, so try TO_DATE
            Optional<String> dt = matchDateTimeString(toDatePattern.matcher(columnValue));

            return dt.map(dateString -> {
                String dateFormat = DateUtil.determineDateFormat(dateString);
                if (EmptyKit.isNotBlank(dateFormat)) {
                    DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern(dateFormat);
                    if (DATE.equals(actualType)) {
                        // if type is DATE, dont convert time zone, just return string
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
                        try {
                            return simpleDateFormat.parse(dateString).getTime() / 1000;
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return Date.from(getDate(dateString, DT_FORMATTER).atZone(zoneId).toInstant());
                } else {
                    return null;
                }
            }).orElse(null);
        }
    }

    public Object getTimestampWithTimezoneField(String columnValue) {
        if (columnValue == null) {
            return null;
        }
        Matcher m = toTimeStampTzPatternTz.matcher(columnValue);
        if (m.find()) {
            String dateString = m.group(1);
            String dateFormat = DateUtil.determineDateFormat(dateString);
            if (EmptyKit.isNotBlank(dateFormat)) {
                DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern(dateFormat).toFormatter();
                return Date.from(ZonedDateTime.parse(m.group(1), dateTimeFormatter).toInstant());
            }
        }
        return null;
    }

    public Object getTimestampWithLocalTimezone(String columnValue) {
        if (columnValue == null) {
            return null;
        }
        Matcher m = toTimeStampTzPatternLocalTz.matcher(columnValue);
        if (m.find()) {
            String dateString = m.group(1);
            String dateFormat = DateUtil.determineDateFormat(dateString);
            if (EmptyKit.isNotBlank(dateFormat)) {
                DateTimeFormatter dateTimeFormatter =
                        new DateTimeFormatterBuilder().appendPattern(dateFormat).toFormatter();
                return Date.from(ZonedDateTime.of(LocalDateTime.parse(dateString, dateTimeFormatter), zoneId).toInstant());
            }
        }
        return null;
    }

    private static Optional<String> matchDateTimeString(Matcher m) {
        if (!m.find()) {
            return Optional.empty();
        }
        return Optional.of(m.group(1));
    }

    LocalDateTime getDate(String s, DateTimeFormatter formatter) {
        return LocalDateTime.parse(s, formatter);
    }

    public Object getTimestamp(Object columnValue, String actualType) {
        String[] strs = ((String) columnValue).split("::");
        if (strs.length == 2) {
            Instant instant = Instant.ofEpochMilli(Long.parseLong(strs[0]) / 1000);
            return Date.from(instant);
        }
        return null;
    }
}
