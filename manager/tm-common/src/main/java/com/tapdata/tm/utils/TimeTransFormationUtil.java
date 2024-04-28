package com.tapdata.tm.utils;

import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapTimeForm;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TimeTransFormationUtil {
    private TimeTransFormationUtil() {
        throw new IllegalStateException("TimeTransFormationUtil class");
    }
    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public static List<String> calculatedTimeRange(LocalDateTime currentDateTime, QueryOperator queryOperator,Long offsetHours){
        List<String> result = new ArrayList<>();
        if(null == queryOperator.getUnit() || null == queryOperator.getForm())return result;
        TapTimeForm tapTimeForm = queryOperator.getForm();
        Long number = queryOperator.getNumber();
        if(  null != offsetHours && offsetHours != 0){
            currentDateTime = currentDateTime.plusHours(offsetHours);
        }
        switch (queryOperator.getUnit()){
            case HOUR:
                LocalDateTime hourStart;
                LocalDateTime hourEnd;
                if(tapTimeForm.equals(TapTimeForm.BEFORE)){
                    hourStart = currentDateTime.minusHours(number).withMinute(0).withSecond(0);
                    hourEnd = hourStart.plusHours(number-1).withMinute(59).withSecond(59);
                }else{
                    hourStart = currentDateTime.withMinute(0).withSecond(0);
                    hourEnd  = currentDateTime;
                }
                result.add(formatDateTime(hourStart));
                result.add(formatDateTime(hourEnd));
                return result;
            case DAY:
                LocalDateTime dayStart;
                LocalDateTime dayEnd;
                if(tapTimeForm.equals(TapTimeForm.BEFORE)){
                    dayStart = currentDateTime.minusDays(number).withHour(0).withMinute(0).withSecond(0);
                    dayEnd = dayStart.plusDays(number-1).withHour(23).withMinute(59).withSecond(59);
                }else{
                    dayStart = currentDateTime.withHour(0).withMinute(0).withSecond(0);
                    dayEnd  = currentDateTime;
                }
                result.add(formatDateTime(dayStart));
                result.add(formatDateTime(dayEnd));
                return result;
            case WEEK:
                LocalDateTime weekStart;
                LocalDateTime weekEnd;
                if(tapTimeForm.equals(TapTimeForm.BEFORE)){
                    weekStart = currentDateTime.minusWeeks(number).with(DayOfWeek.MONDAY).withHour(0).withMinute(0).withSecond(0);
                    weekEnd =  weekStart.plusWeeks(number-1).with(DayOfWeek.SUNDAY).withHour(23).withMinute(59).withSecond(59);
                }else{
                    weekStart = currentDateTime.with(DayOfWeek.MONDAY).withHour(0).withMinute(0).withSecond(0);
                    weekEnd =  currentDateTime;
                }
                result.add(formatDateTime(weekStart));
                result.add(formatDateTime(weekEnd));
                return result;
            case MONTH:
                LocalDateTime monthStart;
                LocalDateTime monthEnd;
                if(tapTimeForm.equals(TapTimeForm.BEFORE)){
                    monthStart = currentDateTime.minusMonths(number).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
                    monthEnd = currentDateTime.minusMonths(1).withDayOfMonth(currentDateTime.minusMonths(1).toLocalDate().lengthOfMonth()).withHour(23).withMinute(59).withSecond(59);
                }else{
                    monthStart = currentDateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
                    monthEnd = currentDateTime;
                }
                result.add(formatDateTime(monthStart));
                result.add(formatDateTime(monthEnd));
                return result;
            case YEAR:
                LocalDateTime yearStart;
                LocalDateTime yearEnd;
                if(tapTimeForm.equals(TapTimeForm.BEFORE)){
                    yearStart = currentDateTime.minusYears(number).withDayOfYear(1).withHour(0).withMinute(0).withSecond(0);
                    yearEnd = currentDateTime.minusYears(1).withDayOfYear(currentDateTime.minusYears(1).toLocalDate().lengthOfYear()).withHour(23).withMinute(59).withSecond(59);
                }else{
                    yearStart = currentDateTime.withDayOfYear(1).withHour(0).withMinute(0).withSecond(0);
                    yearEnd = currentDateTime;
                }
                result.add(formatDateTime(yearStart));
                result.add(formatDateTime(yearEnd));
                return result;
            default:
                return result;
        }
    }




    public static String formatDateTime(LocalDateTime dateTime) {
        return dateTime.format(formatter);
    }

    public static LocalDateTime formatDateTime(String dateTime) {
        return LocalDateTime.parse(dateTime, formatter);
    }
}
