package io.tapdata.util;

import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DateUtil {

    public static long convertTimestamp(long timestamp, TimeZone fromTimeZone, TimeZone toTimeZone) {
        LocalDateTime dt = LocalDateTime.now();
        ZonedDateTime fromZonedDateTime = dt.atZone(fromTimeZone.toZoneId());
        ZonedDateTime toZonedDateTime = dt.atZone(toTimeZone.toZoneId());
        long diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis();

        return timestamp + diff;
    }

    /**
     * timeStamp2Date 时间戳转日期
     */
    public static String timeStamp2Date(String millSeconds, String format) {
        if (millSeconds == null || millSeconds.isEmpty() || millSeconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.parseLong(millSeconds)));
    }

    public static Instant string2Instant(ZoneId zoneId, String dateString) {
        if (EmptyKit.isBlank(dateString) || zoneId == null) {
            throw new NullPointerException();
        }
        int plusIndex = dateString.indexOf("+");
        if (plusIndex > -1) {
            dateString = dateString.replaceAll("\\+.*$", "Z");
        }
        String pattern = DateUtil.determineDateFormat(dateString);
        if (null == pattern) throw new DateTimeException("Not found pattern of " + dateString);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
        Instant instant;
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(dateString, dateTimeFormatter);
            instant = localDateTime.atZone(zoneId).toInstant();
        } catch (Exception e) {
            LocalDate localDate = LocalDate.parse(dateString, dateTimeFormatter);
            instant = localDate.atStartOfDay(zoneId).toInstant();
        }

        return instant;
    }

    public static long long2Nanos(Long times, Integer scale) {
        if (null != scale) {
            switch (scale) {
                case 1:
                case 2:
                case 3:
                    return times * 1000000L; // 毫秒
                case 4:
                case 5:
                case 6:
                    return times * 1000L; // 微秒
                default:
                    break;
            }
        }
        // 默认毫秒
        return times * 1000000L;
    }

    public static Instant nanos2Instant(Long times, ZoneId fromZoneId) {
        Instant instant = Instant.ofEpochSecond(times / 1000000000L, times % 1000000000L);

        // 补时差
        if (null != fromZoneId) {
            TimeZone fromTz = TimeZone.getTimeZone(fromZoneId);
            instant = instant.minusMillis(fromTz.getRawOffset());
        }
        return instant;
    }

    public static Timestamp date2Timestamp(Date date, ZoneId toZoneId) {
        if (date == null) {
            throw new NullPointerException();
        }

        return instant2Timestamp(date.toInstant(), toZoneId);
    }

    public static Timestamp instant2Timestamp(Instant instant, ZoneId toZoneId) {
        if (instant == null) {
            throw new NullPointerException();
        }

        ZonedDateTime zonedDateTime = instant.atZone(toZoneId);
        return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
    }

    public static Time millis2Time(int millis, ZoneId fromZoneId) {
        TimeZone fromTz = TimeZone.getTimeZone(fromZoneId);
        millis = millis - fromTz.getRawOffset();
        return new Time(millis);
    }

    public static int getFullYear(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

    public static int getFullYear(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getYear();
    }

    public static int getYear(Date date) {
        return getFullYear(date);
    }

    public static int getYear(Instant instant) {
        return getFullYear(instant);
    }

    public static int getMonth(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MONTH) + 1;
    }

    public static int getMonth(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getMonthValue();
    }

    public static int getDate(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    public static int getDate(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getDayOfMonth();
    }

    public static int getHours(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static int getHours(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getHour();
    }

    public static int getMinutes(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MINUTE);
    }

    public static int getMinutes(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getMinute();
    }

    public static int getSeconds(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.SECOND);
    }

    public static int getSeconds(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getSecond();
    }

    public static int getMilliSeconds(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MILLISECOND);
    }

    public static int getMilliSeconds(Instant instant) {
        if (instant == null) {
            return 0;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).getNano();
    }

    public static long convertMicros(long micros, TimeZone fromTimeZone, TimeZone toTimeZone) {
        LocalDateTime dt = LocalDateTime.now();
        ZonedDateTime fromZonedDateTime = dt.atZone(fromTimeZone.toZoneId());
        ZonedDateTime toZonedDateTime = dt.atZone(toTimeZone.toZoneId());
        long diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis() * 1000L;

        return micros + diff;
    }


    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {{
        put("^\\d{8}$", "yyyyMMdd");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        put("^\\d{12}$", "yyyyMMddHHmm");
        put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        put("^\\d{14}$", "yyyyMMddHHmmss");
        put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss"); // Oracle Date增量更新格式
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{1}$", "yyyy-MM-dd HH:mm:ss.S");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{2}$", "yyyy-MM-dd HH:mm:ss.SS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{3}$", "yyyy-MM-dd HH:mm:ss.SSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{4}$", "yyyy-MM-dd HH:mm:ss.SSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{5}$", "yyyy-MM-dd HH:mm:ss.SSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{6}$", "yyyy-MM-dd HH:mm:ss.SSSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{7}$", "yyyy-MM-dd HH:mm:ss.SSSSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{8}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{9}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        put("^\\d{15}$", "yyyyMMddHHmmssS");
        put("^\\d{16}$", "yyyyMMddHHmmssSS");
        put("^\\d{17}$", "yyyyMMddHHmmssSSS");
        put("^\\d{18}$", "yyyyMMddHHmmssSSSS");
        put("^\\d{19}$", "yyyyMMddHHmmssSSSSS");
        put("^\\d{20}$", "yyyyMMddHHmmssSSSSSS");
        put("^\\d{14}\\.\\d{6}$", "yyyyMMddHHmmss.SSSSSS");
        put("^\\d{21}$", "yyyyMMddHHmmssSSSSSSS");
        put("^\\d{22}$", "yyyyMMddHHmmssSSSSSSSS");
        put("^\\d{23}$", "yyyyMMddHHmmssSSSSSSSSS");
        put("^\\d{6}$", "HHmmss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,6}$", "yyyy/MM/dd HH:mm:ss.SSSSSS");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{7,9}$", "yyyy/MM/dd HH:mm:ss.SSSSSSSSS");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
        put("^\\d{1,2}/(0?[1-9]|1[012])/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd/MM/yyyy HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}Z$", "yyyy-MM-dd'T'HH:mm:ss'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1}Z$", "yyyy-MM-dd'T'HH:mm:ss.S'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{2}Z$", "yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        // 4 and 5 precision is provided because formatter with 7 precision will break precision 4 and 5
        // more detail at: https://stackoverflow.com/questions/68411113/text-2021-06-22t182703-5577z-could-not-be-parsed-at-index-20
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{4}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{5}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSSSS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6,7}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,3}[+-]\\d{2}$", "yyyy-MM-dd'T'HH:mm:ss.SSSX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,3}[+-]\\d{4}$", "yyyy-MM-dd'T'HH:mm:ss.SSSXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss z");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{9}[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSSSz");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\s[+-]\\d{2}\\d{2}$", "yyyy-MM-dd HH:mm:ss Z");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss. z");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\s[+-]\\d{2}\\d{2}$", "yyyy-MM-dd HH:mm:ss. Z");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.S XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{2}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{4}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{5}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{7}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{8}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{9}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        put("^([a-zA-Z]{3})\\s([a-zA-Z]{3})\\s(\\d{2})\\s(\\d{2}):(\\d{2}):(\\d{2})\\s(([a-zA-Z]+)|(GMT(\\+|-)\\d{2}:00))\\s(\\d{4})$", "EEE MMM dd HH:mm:ss zzz yyyy");
    }};

    private DateUtil() {
        // Utility class, hide the constructor.
    }

    // Converters ---------------------------------------------------------------------------------

    /**
     * Convert the given date to a Calendar object. The TimeZone will be derived from the local
     * operating system's timezone.
     *
     * @param date The date to be converted to Calendar.
     * @return The Calendar object set to the given date and using the local timezone.
     */
    public static Calendar toCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);
        return calendar;
    }

    /**
     * Convert the given date to a Calendar object with the given timezone.
     *
     * @param date     The date to be converted to Calendar.
     * @param timeZone The timezone to be set in the Calendar.
     * @return The Calendar object set to the given date and timezone.
     */
    public static Calendar toCalendar(Date date, TimeZone timeZone) {
        Calendar calendar = toCalendar(date);
        calendar.setTimeZone(timeZone);
        return calendar;
    }

    public static Object parseInstant(String dateString) {
        String dateFormat = determineDateFormat(dateString);
        if (EmptyKit.isNull(dateFormat)) {
            return dateString;
        }
        DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern(dateFormat);
        if (dateFormat.contains("H")) {
            return LocalDateTime.parse(dateString, DT_FORMATTER).atZone(TimeZone.getDefault().toZoneId()).toInstant();
        } else {
            return LocalDate.parse(dateString, DT_FORMATTER).atStartOfDay(TimeZone.getDefault().toZoneId()).toInstant();
        }
    }

    /**
     * Parse the given date string to date object and return a date instance based on the given
     * date string. This makes use of the {@link DateUtil#determineDateFormat(String)} to determine
     * the SimpleDateFormat pattern to be used for parsing.
     *
     * @param dateObj The date string to be parsed to date object.
     * @return The parsed date object.
     * the given date string or its actual date is invalid based on the date format pattern.
     */
    public static Object parse(Object dateObj) {
        Date date = null;
        if (dateObj == null) {
            return date;
        }
        try {
            if (dateObj instanceof String) {
                String dateFormat = determineDateFormat((String) dateObj);

                assert dateFormat != null;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
                date = simpleDateFormat.parse((String) dateObj);

            } else {
                // only can convert from long value.
                date = new Date(new BigDecimal(String.valueOf(dateObj)).longValue());
            }
        } catch (Exception e) {
            return dateObj;
        }
        return date;
    }

    public static Object parse(Object dateObj, Integer timezone) {
        Date date = null;
        if (dateObj == null) {
            return date;
        }
        try {
            if (timezone == null) {
                timezone = 8;
            }
            TimeZone timeZone = getTimeZone(timezone);
            if (dateObj instanceof String) {
                String dateFormat = determineDateFormat((String) dateObj);

                date = parse((String) dateObj, dateFormat, timeZone);

            } else {
                // only can convert from long value.
                date = new Date(new BigDecimal(String.valueOf(dateObj)).longValue());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String formatStr = sdf.format(date);
                date = (Date) parse(formatStr, timezone);
//                Calendar calendar = toCalendar(date)
//                calendar.setTimeZone(timeZone);
//                date = calendar.getTime();
            }
        } catch (Exception e) {
            return dateObj;
        }
        return date;
    }

//    public static Date parse(Long millis) {
//        if (millis != null) {
//            return new Date(millis);
//        }
//
//        return null;
//    }

    /**
     * Validate the actual date of the given date string based on the given date format pattern and
     * return a date instance based on the given date string.
     *
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return The parsed date object.
     * @throws ParseException If the given date string or its actual date is invalid based on the
     *                        given date format pattern.
     * @see SimpleDateFormat
     */
//    public static Date parse(String dateString, String dateFormat) throws ParseException {
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
//        simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
//        return simpleDateFormat.parse(dateString);
//    }
    public static Date parse(String dateString, String dateFormat, TimeZone timeZone) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        //simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
        if (timeZone == null) {
            timeZone = TimeZone.getDefault();
        }
        simpleDateFormat.setTimeZone(timeZone);
        return simpleDateFormat.parse(dateString);
    }

    public static TimeZone getTimeZone(Integer timeZoneOffset) {
        if (timeZoneOffset == null) {
            return TimeZone.getDefault();
        }
        StringBuilder sb = new StringBuilder("GMT");
        String str = String.valueOf(timeZoneOffset);
        if (str.length() <= 1 || (str.contains("-") && str.length() <= 2)) {
            if (str.contains("-")) {
                sb.append("-0").append(Math.abs(timeZoneOffset));
            } else {
                sb.append("+0").append(timeZoneOffset);
            }
        } else {
            sb.append(str);
        }
        sb.append(":00");

        return TimeZone.getTimeZone(sb.toString());
    }

    // Validators ---------------------------------------------------------------------------------

    //    public static boolean isValidDate(String dateString) {
//        try {
//            parse(dateString);
//            return true;
//        } catch (ParseException e) {
//            return false;
//        }
//    }

    //    public static boolean isValidDate(String dateString, String dateFormat) {
//        try {
//            parse(dateString, dateFormat);
//            return true;
//        } catch (ParseException e) {
//            return false;
//        }
//    }

    // Checkers -----------------------------------------------------------------------------------

    /**
     * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
     * format is unknown. You can simply extend DateUtil with more formats if needed.
     *
     * @param dateString The date string to determine the SimpleDateFormat pattern for.
     * @return The matching SimpleDateFormat pattern, or null if format is unknown.
     * @see SimpleDateFormat
     */
    public static String determineDateFormat(String dateString) {
        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (dateString.matches(regexp)) {
                return DATE_FORMAT_REGEXPS.get(regexp);
            }
        }
        return null; // Unknown format.
    }

    // Changers -----------------------------------------------------------------------------------

    /**
     * Add the given amount of years to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addYears(Calendar, int)} and then converts back to date.
     *
     * @param date  The date to add the given amount of years to.
     * @param years The amount of years to be added to the given date. Negative values are also
     *              allowed, it will just go back in time.
     */
    public static Date addYears(Date date, int years) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addYears(calendar, years);
        return calendar.getTime();
    }

    public static Date addYears(Instant instant, int years) {
        return addYears(Date.from(instant), years);
    }

    /**
     * Add the given amount of months to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addMonths(Calendar, int)} and then converts back to date.
     *
     * @param date   The date to add the given amount of months to.
     * @param months The amount of months to be added to the given date. Negative values are also
     *               allowed, it will just go back in time.
     */
    public static Date addMonths(Date date, int months) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addMonths(calendar, months);
        return calendar.getTime();
    }

    public static Date addMonths(Instant instant, int months) {
        return addMonths(Date.from(instant), months);
    }

    /**
     * Add the given amount of days to the given date. It actually converts the date to Calendar and
     * calls {@link CalendarUtil#addDays(Calendar, int)} and then converts back to date.
     *
     * @param date The date to add the given amount of days to.
     * @param days The amount of days to be added to the given date. Negative values are also
     *             allowed, it will just go back in time.
     */
    public static Date addDays(Date date, int days) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addDays(calendar, days);
        return calendar.getTime();
    }

    public static Date addDays(Instant instant, int days) {
        return addDays(Date.from(instant), days);
    }

    /**
     * Add the given amount of hours to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addHours(Calendar, int)} and then converts back to date.
     *
     * @param date  The date to add the given amount of hours to.
     * @param hours The amount of hours to be added to the given date. Negative values are also
     *              allowed, it will just go back in time.
     */
    public static Date addHours(Date date, int hours) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addHours(calendar, hours);
        return calendar.getTime();
    }

    public static Date addHours(Instant instant, int hours) {
        return addHours(Date.from(instant), hours);
    }

    /**
     * Add the given amount of minutes to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addMinutes(Calendar, int)} and then converts back to date.
     *
     * @param date    The date to add the given amount of minutes to.
     * @param minutes The amount of minutes to be added to the given date. Negative values are also
     *                allowed, it will just go back in time.
     */
    public static Date addMinutes(Date date, int minutes) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addMinutes(calendar, minutes);
        return calendar.getTime();
    }

    public static Date addMinutes(Instant instant, int minutes) {
        return addMinutes(Date.from(instant), minutes);
    }

    /**
     * Add the given amount of seconds to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addSeconds(Calendar, int)} and then converts back to date.
     *
     * @param date    The date to add the given amount of seconds to.
     * @param seconds The amount of seconds to be added to the given date. Negative values are also
     *                allowed, it will just go back in time.
     */
    public static Date addSeconds(Date date, int seconds) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addSeconds(calendar, seconds);
        return calendar.getTime();
    }

    public static Date addSeconds(Instant instant, int seconds) {
        return addSeconds(Date.from(instant), seconds);
    }

    /**
     * Add the given amount of millis to the given date. It actually converts the date to Calendar
     * and calls {@link CalendarUtil#addMillis(Calendar, int)} and then converts back to date.
     *
     * @param date   The date to add the given amount of millis to.
     * @param millis The amount of millis to be added to the given date. Negative values are also
     *               allowed, it will just go back in time.
     */
    public static Date addMillis(Date date, int millis) {
        Calendar calendar = toCalendar(date);
        CalendarUtil.addMillis(calendar, millis);
        return calendar.getTime();
    }

    public static Date addMillis(Instant instant, int millis) {
        return addMillis(Date.from(instant), millis);
    }

    // Comparators --------------------------------------------------------------------------------

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year. It actually
     * converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameYear(Calendar, Calendar)}.
     *
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year.
     * @see CalendarUtil#sameYear(Calendar, Calendar)
     */
    public static boolean sameYear(Date one, Date two) {
        return CalendarUtil.sameYear(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year and month. It
     * actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameMonth(Calendar, Calendar)}.
     *
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year and month.
     * @see CalendarUtil#sameMonth(Calendar, Calendar)
     */
    public static boolean sameMonth(Date one, Date two) {
        return CalendarUtil.sameMonth(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month and day. It
     * actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameDay(Calendar, Calendar)}.
     *
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month and day.
     * @see CalendarUtil#sameDay(Calendar, Calendar)
     */
    public static boolean sameDay(Date one, Date two) {
        return CalendarUtil.sameDay(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day and
     * hour. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameHour(Calendar, Calendar)}.
     *
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month, day and hour.
     * @see CalendarUtil#sameHour(Calendar, Calendar)
     */
    public static boolean sameHour(Date one, Date two) {
        return CalendarUtil.sameHour(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day, hour
     * and minute. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameMinute(Calendar, Calendar)}.
     *
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month, day, hour and minute.
     * @see CalendarUtil#sameMinute(Calendar, Calendar)
     */
    public static boolean sameMinute(Date one, Date two) {
        return CalendarUtil.sameMinute(toCalendar(one), toCalendar(two));
    }

    /**
     * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day, hour,
     * minute and second. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#sameSecond(Calendar, Calendar)}.
     *
     * @param one The one date.
     * @param two The other date.
     * @return True if the two given dates are dated on the same year, month, day, hour, minute and
     * second.
     * @see CalendarUtil#sameSecond(Calendar, Calendar)
     */
    public static boolean sameSecond(Date one, Date two) {
        return CalendarUtil.sameSecond(toCalendar(one), toCalendar(two));
    }

    // Calculators --------------------------------------------------------------------------------

    /**
     * Retrieve the amount of elapsed years between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedYears(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed years between the two given dates
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedYears(Calendar, Calendar)
     */
    public static int elapsedYears(Date before, Date after) {
        return CalendarUtil.elapsedYears(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed months between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedMonths(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed months between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedMonths(Calendar, Calendar)
     */
    public static int elapsedMonths(Date before, Date after) {
        return CalendarUtil.elapsedMonths(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed days between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedDays(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed days between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedDays(Calendar, Calendar)
     */
    public static int elapsedDays(Date before, Date after) {
        return CalendarUtil.elapsedDays(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed hours between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedHours(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed hours between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedHours(Calendar, Calendar)
     */
    public static int elapsedHours(Date before, Date after) {
        return CalendarUtil.elapsedHours(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed minutes between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedMinutes(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed minutes between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedMinutes(Calendar, Calendar)
     */
    public static int elapsedMinutes(Date before, Date after) {
        return CalendarUtil.elapsedMinutes(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed seconds between the two given dates. It actually converts the
     * both dates to Calendar and calls {@link CalendarUtil#elapsedSeconds(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed seconds between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedSeconds(Calendar, Calendar)
     */
    public static int elapsedSeconds(Date before, Date after) {
        return CalendarUtil.elapsedSeconds(toCalendar(before), toCalendar(after));
    }

    /**
     * Retrieve the amount of elapsed milliseconds between the two given dates. It actually converts
     * the both dates to Calendar and calls {@link CalendarUtil#elapsedMillis(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The amount of elapsed milliseconds between the two given dates.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedMillis(Calendar, Calendar)
     */
    public static long elapsedMillis(Date before, Date after) {
        return CalendarUtil.elapsedMillis(toCalendar(before), toCalendar(after));
    }

    /**
     * Calculate the total of elapsed time from years up to seconds between the two given dates. It
     * Returns an int array with the elapsed years, months, days, hours, minutes and seconds
     * respectively. It actually converts the both dates to Calendar and calls
     * {@link CalendarUtil#elapsedTime(Calendar, Calendar)}.
     *
     * @param before The first date with expected date before the second date.
     * @param after  The second date with expected date after the first date.
     * @return The elapsed time between the two given dates in years, months, days, hours, minutes
     * and seconds.
     * @throws IllegalArgumentException If the first date is dated after the second date.
     * @see CalendarUtil#elapsedTime(Calendar, Calendar)
     */
    public static int[] elapsedTime(Date before, Date after) {
        return CalendarUtil.elapsedTime(toCalendar(before), toCalendar(after));
    }

    public static Date epochDaysToDate(Object data) throws ParseException {
        Integer passDays = (Integer) data;
        if (passDays != null) {
            long millis = TimeUnit.DAYS.toMillis(passDays);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            return sdf.parse(sdf.format(millis));
        }
        return null;
    }

    public static Date epochMillsToDate(Object data, TimeZone timeZone) throws ParseException {
        Long millis = (Long) data;
        if (data != null) {
            long convertTimestamp = convertTimestamp(millis, TimeZone.getDefault(), timeZone);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            return sdf.parse(sdf.format(convertTimestamp));
        }
        return null;
    }

    public static Date epochMicrosToDate(Object data, TimeZone timeZone) throws ParseException {
        Long micros = (Long) data;
        if (data != null) {
            long convertMicros = convertMicros(micros, TimeZone.getDefault(), timeZone);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
            return sdf.parse(sdf.format(convertMicros));
        }
        return null;
    }

    public static String microseondsToStringTime(Object data) {
        Long passMidros = (Long) data;
        StringBuilder result = new StringBuilder();
        if (passMidros != null) {
            String ssssss = String.valueOf(passMidros % 1000000);
            long seconds = passMidros / 1000000;
            String ss = String.valueOf(seconds % 60);
            long min = seconds / 60;
            String mm = String.valueOf(min % 60);
            long hour = min / 60;
            String hh = String.valueOf(hour % 24);
            StringKit.leftPad(ssssss, 6, "0");

            result.append(StringKit.leftPad(hh, 2, "0")).append(":")
                    .append(StringKit.leftPad(mm, 2, "0")).append(":")
                    .append(StringKit.leftPad(ss, 2, "0")).append(".")
                    .append(StringKit.leftPad(ssssss, 6, "0"));
        }
        return result.toString();
    }
}
