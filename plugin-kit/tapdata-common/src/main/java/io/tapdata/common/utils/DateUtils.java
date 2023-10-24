package io.tapdata.common.utils;

import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    public static Date getDateWithoutSeconds(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return new Date(c.getTimeInMillis());
    }
    public static Date getDateWithoutMinute(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.MINUTE, 0);
        return new Date(c.getTimeInMillis());
    }
    public static Date getDateWithoutHour(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);
        return new Date(c.getTimeInMillis());
    }
    public static Date getDateWithoutDay(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);
        return new Date(c.getTimeInMillis());
    }
    public static void main(String... args) {
        System.out.println("time " + System.currentTimeMillis());
        System.out.println("time " + new Date().getTime());
        System.out.println("date " + new Date());
        System.out.println("getDateWithoutSeconds " + DateUtils.getDateWithoutSeconds(new Date()));
        System.out.println("getDateWithoutMinute " + DateUtils.getDateWithoutMinute(new Date()));
        System.out.println("getDateWithoutHour " + DateUtils.getDateWithoutHour(new Date()));
        System.out.println("getDateWithoutDay " + DateUtils.getDateWithoutDay(new Date()));
    }
}
