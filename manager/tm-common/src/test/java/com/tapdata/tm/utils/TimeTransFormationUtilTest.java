package com.tapdata.tm.utils;

import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapTimeForm;
import io.tapdata.pdk.apis.entity.TapTimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

public class TimeTransFormationUtilTest {
    @Nested
    class calculatedTimeRange{
        LocalDateTime currentDateTime = TimeTransFormationUtil.formatDateTime("2024-04-25 19:20:00");
        @DisplayName("Tested for the past hour")
        @Test
        void test(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.HOUR);
            queryOperator.setForm(TapTimeForm.BEFORE);
            queryOperator.setNumber(1L);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-25 18:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 18:59:59",result.get(1));
        }

        @DisplayName("Test one day ago")
        @Test
        void test1(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.DAY);
            queryOperator.setForm(TapTimeForm.BEFORE);
            queryOperator.setNumber(1L);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-24 00:00:00",result.get(0));
            Assertions.assertEquals("2024-04-24 23:59:59",result.get(1));
        }

        @DisplayName("Test one Week ago")
        @Test
        void test2(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.WEEK);
            queryOperator.setForm(TapTimeForm.BEFORE);
            queryOperator.setNumber(1L);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-15 00:00:00",result.get(0));
            Assertions.assertEquals("2024-04-21 23:59:59",result.get(1));
        }

        @DisplayName("Test one Month ago")
        @Test
        void test3(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.MONTH);
            queryOperator.setForm(TapTimeForm.BEFORE);
            queryOperator.setNumber(1L);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-03-01 00:00:00",result.get(0));
            Assertions.assertEquals("2024-03-31 23:59:59",result.get(1));
        }

        @DisplayName("Test one Year ago")
        @Test
        void test4(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.YEAR);
            queryOperator.setForm(TapTimeForm.BEFORE);
            queryOperator.setNumber(1L);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2023-01-01 00:00:00",result.get(0));
            Assertions.assertEquals("2023-12-31 23:59:59",result.get(1));
        }
        @DisplayName("Test that hour")
        @Test
        void test5(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.HOUR);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-25 19:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 19:20:00",result.get(1));
        }

        @DisplayName("Test that day")
        @Test
        void test6(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.DAY);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-25 00:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 19:20:00",result.get(1));
        }

        @DisplayName("Test that Week")
        @Test
        void test7(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.WEEK);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-22 00:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 19:20:00",result.get(1));
        }

        @DisplayName("Test that Month")
        @Test
        void test8(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.MONTH);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-04-01 00:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 19:20:00",result.get(1));
        }
        @DisplayName("Test that year")
        @Test
        void test9(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.YEAR);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,null);
            Assertions.assertEquals("2024-01-01 00:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 19:20:00",result.get(1));
        }

        @DisplayName("Test that day,Time difference minus 8")
        @Test
        void test10(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.HOUR);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,-8L);
            Assertions.assertEquals("2024-04-25 11:00:00",result.get(0));
            Assertions.assertEquals("2024-04-25 11:20:00",result.get(1));
        }

        @DisplayName("Test that day,Time difference add 8")
        @Test
        void test11(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.HOUR);
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,8L);
            Assertions.assertEquals("2024-04-26 03:00:00",result.get(0));
            Assertions.assertEquals("2024-04-26 03:20:00",result.get(1));
        }

        @DisplayName("TimeUnit is null")
        @Test
        void test12(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setForm(TapTimeForm.CURRENT);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,8L);
            Assertions.assertEquals(0,result.size());
        }

        @DisplayName("TimeForm is null")
        @Test
        void test13(){
            QueryOperator queryOperator = new QueryOperator();
            queryOperator.setUnit(TapTimeUnit.HOUR);
            List<String> result = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,8L);
            Assertions.assertEquals(0,result.size());
        }

    }
}
