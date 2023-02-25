package com.tapdata.tm.utils;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class TimeUtil {

    public static Date getBeginOfDay(Date time) {
        Date beginOfDate = null;
        try {
            //TODO 强制设置时区
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String s = sdf.format(time);
            beginOfDate = sdf.parse(s);
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return beginOfDate;
    }



    public static Date getEndOfDay(Date time) {
        Date endDate = null;
        try {
            endDate = DateUtil.endOfDay(time);
           /* todayEnd.set(Calendar.HOUR, 23);
            todayEnd.set(Calendar.MINUTE, 59);
            todayEnd.set(Calendar.SECOND, 59);
            todayEnd.set(Calendar.MILLISECOND, 999);
            return todayEnd.getTime().getTime();*/
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return endDate;
    }



    public static List<Date> getEveryHour(Date beginDate, Date endDate) {
        Calendar tt = Calendar.getInstance();
        tt.setTime(beginDate);
        Calendar t2 = Calendar.getInstance();
        t2.setTime(endDate);
        List<Date> dateList = new ArrayList<Date>();
        for (; tt.compareTo(t2) < 0; tt.add(Calendar.HOUR, 1)) {
            dateList.add(cleanTimeAfterHour(tt.getTime()));
        }
        return dateList;
    }


    /**
     * 获取时间周期内的每一天
     * @param beginDate
     * @param endDate
     * @return
     */
    public static List<Date> getEveryDay(Date beginDate, Date endDate) {
        Calendar tt = Calendar.getInstance();
        tt.setTime(beginDate);
        Calendar t2 = Calendar.getInstance();
        t2.setTime(endDate);
        List<Date> dateList = new ArrayList<Date>();
        for (; tt.compareTo(t2) < 0; tt.add(Calendar.DATE, 1)) {
            dateList.add(cleanTimeAfterHour(tt.getTime()));
        }
        return dateList;
    }

    /**
     * 把月后面的时间值清零，以便获得只有整数月的date
     *
     * @return
     */
    public static Date cleanTimeAfterMonth(Date date) {
        Date dateWithoutDay = date;
        try {
            //TODO 强制设置时区
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
            String s = sdf.format(date);
            dateWithoutDay = sdf.parse(s);
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return dateWithoutDay;
    }

    /**
     * 把天后面的时间值清零，以便获得只有整数天的date
     *
     * @return
     */
    public static Date cleanTimeAfterDay(Date date) {
        Date dateWithoutHour = date;
        try {
            //TODO 强制设置时区
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String s = sdf.format(date);
            dateWithoutHour = sdf.parse(s);
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return dateWithoutHour;
    }


    /**
     * 把小时后面的时间值清零，以便获得只有整数小时的date
     *
     * @return
     */
    public static Date cleanTimeAfterHour(Date date) {
        Date dateWithoutMinute = date;
        try {
            //TODO 强制设置时区
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
            String s = sdf.format(date);
            dateWithoutMinute = sdf.parse(s);
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return dateWithoutMinute;
    }

    /**
     * 把分钟后面的时间值清零，以便获得只有整数分钟的date
     *
     * @return
     */
    public static Date cleanTimeAfterMinute(Date date) {
        Date dateWithoutSecond = date;
        try {
            //TODO 强制设置时区
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            String s = sdf.format(date);
            dateWithoutSecond = sdf.parse(s);
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return dateWithoutSecond;
    }

    public static Date cleanTimeAfterSecond(Date date) {
        Date dateWithoutSecond = date;
        try {
            //TODO 强制设置时区
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String s = sdf.format(date);
            dateWithoutSecond = sdf.parse(s);
        } catch (Exception e) {
            log.error("情况秒出错", e);
        }
        return dateWithoutSecond;
    }



    public static Date format(Date date, String datePattern) {
        Date retDate = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(datePattern);//显示20171027格式

            String dateSrtr = sdf.format(date);
            retDate = sdf.parse(dateSrtr);
        } catch (Exception e) {
            log.error("date format error", e);
        }
        return retDate;
    }


    public static Long parseDayToSeconds(Integer day){
        Long seconds= Long.valueOf(day*24*60*60);
        return seconds;
    }
    public static Long parseSecondsToDay(Long seconds){
        Long day= seconds/(24*60*60);
        return  day;
    }

}
