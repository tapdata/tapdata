package com.tapdata.tm.modules.constant;


import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.NumberUtil;
import com.tapdata.tm.base.exception.BizException;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public enum ParamTypeEnum {

    NUMBER("Number"),
    STRING("String"),
    DATE("Date"),

    DATE_TIME("DateTime"),
    TIME("Time"),
    BOOLEAN("Boolean");

    public String type;

    private ParamTypeEnum(String type) {
        this.type = type;
    }

    public static boolean isValid(String type,String defaultValue) {
        if(StringUtils.isBlank(type))
            throw new BizException("params's type can't be null");
        if (StringUtils.isBlank(defaultValue)) {
            long count = Arrays.stream(values()).filter(p -> p.type.equalsIgnoreCase(type)).count();
            if(count<1) throw new BizException(type + " type is nonsupport");
            return true;
        }
        defaultValue=defaultValue.trim();
        for (ParamTypeEnum value : values()) {
            if (value.type.equalsIgnoreCase(type)) {
                switch (value) {
                    case NUMBER:
                        if(!NumberUtil.isNumber(defaultValue))
                            throw new BizException(defaultValue + " is not be Number");
                        break;
                    case STRING:
                        break;
                    case DATE:
                        try {
                            //yyyy-MM-dd
                            LocalDate localDate = LocalDateTimeUtil.parseDate(defaultValue);
                        } catch (Exception e) {
                            throw new BizException(defaultValue+" must be 'yyyy-MM-dd'format");
                        }
                        break;
                    case DATE_TIME:
                        try {
                            DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            LocalDateTime parse = LocalDateTime.parse(defaultValue,dateTimeFormatter1);
                        } catch (Exception e) {
                            throw new BizException(defaultValue+" must be 'yyyy-MM-dd HH:mm:ss' format");
                        }
                        break;
                    case TIME:
                        try {
                            LocalTime parse = LocalTime.parse(defaultValue);
                        } catch (Exception e) {
                            throw new BizException(defaultValue+" must be 'HH:mm:ss' format");
                        }
                        break;
                    case BOOLEAN:
                        if (!java.lang.Boolean.parseBoolean(defaultValue) && !"1".equals(defaultValue) && !"0".equals(defaultValue)) {
                            throw new BizException(defaultValue+" is not be boolean");
                        }
                        break;

                }
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        String str = "hello";
        boolean numeric = StringUtils.isNumeric(str);
        System.out.println(numeric);

        boolean number = NumberUtil.isNumber(str);
        System.out.println("number:" + number);

        LocalDate now = LocalDate.now();
        System.out.println(now);

        LocalDate localDate = LocalDateTimeUtil.parseDate("2022-09-01");//格式统一
        System.out.println(localDate);

        LocalDateTime now1 = LocalDateTime.now();
        System.out.println(now1);

        DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime parse = LocalDateTime.parse("2022-09-01 16:00:52",dateTimeFormatter1);
        System.out.println("parse:" + parse);

        LocalTime now2 = LocalTime.now();
        System.out.println("now2:" + now2);

        LocalTime parse1 = LocalTime.parse("16:06:21");
        System.out.println("parse1:" + parse1);

        boolean b = java.lang.Boolean.parseBoolean("false1");
        System.out.println("b:" + b);
    }
}
