package com.tapdata.processor;


import org.junit.Assert;
import org.junit.Test;
import java.time.LocalDate;
import java.time.ZoneId;

import java.util.Date;


public class Convert2DateUtilTest {

    /**
     * 测试符合日期格式 yyyy-MM-dd 的字符串转化为Date类型
     * Example input: {@param inputValue:"2023-11-07"}
     * Expected output: value:2023-11-07
     */
    @Test
    public void validDateFormatConvertTest(){
        int year = 2023;
        int month = 11;
        int day = 7;
        LocalDate localDate = LocalDate.of(year, month, day);
        Date expectedDate = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        String inputValue = "2023-11-07";
        Date outputDate = FieldProcessUtil.convert2Date(inputValue);
        Assert.assertNotNull(outputDate);
        Assert.assertEquals(expectedDate, outputDate);
    }

    /**
     * 测试不符合格式 yyyy-MM-dd 的字符串格式转化为Date类型
     * Example input: {@param inputValue: "DateTime nano 0 seconds 1327149189 timeZone null"}
     * Expected output: RuntimeException(Convert value inputValue to Date failed , error message: DateTime constructor illegal dateStr: inputValue)
     */
    @Test(expected = RuntimeException.class)
    public void illegalDateFormatConvertTest() {
        String inputValue = "DateTime nano 0 seconds 1327149189 timeZone null";
        FieldProcessUtil.convert2Date(inputValue);
    }


    /**
     * 测试Long类型转为Date类型
     * Example input:  {@param inputValue:1699286400000}
     * Expected output: value.getTime():1699286400000
     */
    @Test
    public void longTypeConvertTest() {
        Long expectedDate = 1699286400000L;
        Date outputDate = FieldProcessUtil.convert2Date(expectedDate);
        Assert.assertNotNull(outputDate);
        Assert.assertTrue(expectedDate == outputDate.getTime());
    }

    /**
     * 测试null值转换为Date类型
     * Example input : {@param inputValue:null}
     * Expected output: null
     */
    @Test
    public void nullValueConvertTest() {
        String inputValue = null;
        Date outputDate = FieldProcessUtil.convert2Date(inputValue);
        Assert.assertNull(outputDate);
    }

    /**
     * 测试空字符串转换为Date类型
     * Example input : {@param inputValue:""}
     * Expected output: RuntimeException(Convert value inputValue to Date failed , error message: DateTime constructor illegal dateStr: inputValue)
     */
    @Test(expected = RuntimeException.class)
    public void emptyValueConvertTest() {
        String inputValue = "";
        FieldProcessUtil.convert2Date(inputValue);
    }

    /**
     * 测试两边为空格空格，中间为符合日期格式的字符串转为Date类型
      * Example input : {@param inputValue:"  2023-11-06  "}
      * Expected output: 2023-11-06
     */
    @Test
    public void emptyOnBothSidesValidOnMilddleStringConvertTest() {
        int year = 2023;
        int month = 11;
        int day = 6;
        LocalDate localDate = LocalDate.of(year, month, day);
        Date expectedDate = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        String inputValue = " 2023-11-06 ";
        Date outputDate = FieldProcessUtil.convert2Date(inputValue);
        Assert.assertNotNull(outputDate);
        Assert.assertEquals(expectedDate, outputDate);
    }

    /**
     * 测试两边为空格，中间为不合法字符串格式日期的字符串转为Date类型
     * Example input : {@param inputValue:"  illegal  "}
     * Example output: RuntimeException(Convert value inputValue to Date failed , error message: DateTime constructor illegal dateStr: inputValue)
     */
    @Test(expected = RuntimeException.class)
    public void emptyOnBothSidesIllegalOnMilddleStringConvertTest() {
        String inputValue = " illegal ";
        FieldProcessUtil.convert2Date(inputValue);
    }


}
