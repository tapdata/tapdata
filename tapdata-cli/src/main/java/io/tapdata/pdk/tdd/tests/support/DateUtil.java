package io.tapdata.pdk.tdd.tests.support;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.sss";
    public static final String DATE_TIME_GMT_FORMAT = "yyyy-MM-dd HH:mm:ss.ssssss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String YEAR_FORMAT = "yyyy";
    public static final String TIME_FORMAT = "HH:mm:ss";

    public static String dateTimeToStr(Date date) {
        return DateUtil.dateToStr(date, DATE_TIME_FORMAT);
    }

    public static String dateTimeToStr() {
        return DateUtil.dateToStr(new Date(), DATE_TIME_FORMAT);
    }

    public static String timeToStr(Date date) {
        return DateUtil.dateToStr(date, TIME_FORMAT);
    }

    public static String timeToStr() {
        return DateUtil.dateToStr(new Date(), TIME_FORMAT);
    }

    public static String dateToStr(Date date) {
        return DateUtil.dateToStr(date, DATE_FORMAT);
    }

    public static String dateToStr() {
        return DateUtil.dateToStr(new Date(), DATE_FORMAT);
    }

    public static String yearToStr(Date date) {
        return DateUtil.dateToStr(date, YEAR_FORMAT);
    }

    public static String yearToStr() {
        return DateUtil.dateToStr(new Date(), YEAR_FORMAT);
    }

    public static String dateToStr(Date date, String format) {
        return DateUtil.simpleFormat(date, format, DateUtil.globalTimeZone());
    }

    public static String dateToStr(Date date, String format, TimeZone timeZone) {
        return DateUtil.simpleFormat(date, format, timeZone);
    }

    public static String simpleFormat(Date date, String format, TimeZone timeZone) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        simpleDateFormat.setTimeZone(timeZone);
        return (simpleDateFormat).format(date);
    }

    public static TimeZone globalTimeZone() {
        ZoneId zoneId = ZoneId.ofOffset("GMT", ZoneOffset.ofHours(8));
        return TimeZone.getTimeZone(zoneId);
    }
}
