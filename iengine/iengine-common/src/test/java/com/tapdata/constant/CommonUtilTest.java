package com.tapdata.constant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommonUtilTest {
    @Nested
    class compareBooleanTest{
        @DisplayName("test one value is boolean")
        @Test
        void test1(){
            boolean result = CommonUtil.compareBoolean(true, null);
            assertEquals(false,result);
        }
        @DisplayName("test two value is null")
        @Test
        void test2(){
            boolean result = CommonUtil.compareBoolean(null, null);
            assertEquals(true,result);
        }
        @DisplayName("test two value is boolean")
        @Test
        void test3(){
            boolean result = CommonUtil.compareBoolean(true, false);
            assertEquals(false,result);
        }
        @DisplayName("test one value is 1")
        @Test
        void test4(){
            boolean result = CommonUtil.compareBoolean(true, "1");
            assertEquals(true,result);
        }
    }
    @Nested
    class testToBoolean{
        @DisplayName("test Number not 1 or 0")
        @Test
        void test1(){
            Boolean aBoolean = CommonUtil.toBoolean(2);
            assertEquals(null,aBoolean);
        }
        @DisplayName("test Number 1")
        @Test
        void test2(){
            Boolean aBoolean = CommonUtil.toBoolean(1);
            assertEquals(true,aBoolean);
        }
        @DisplayName("test Number 0")
        @Test
        void test3(){
            Boolean aBoolean = CommonUtil.toBoolean(0);
            assertEquals(false,aBoolean);
        }
        @DisplayName("test Number 0.0")
        @Test
        void test4(){
            Boolean aBoolean = CommonUtil.toBoolean(0.0);
            assertEquals(false,aBoolean);
        }
        @DisplayName("test BigDecimal 1")
        @Test
        void test5(){
            Boolean aBoolean = CommonUtil.toBoolean(new BigDecimal("1"));
            assertEquals(true,aBoolean);
        }
        @DisplayName("test BigDecimal 0")
        @Test
        void test6(){
            Boolean aBoolean = CommonUtil.toBoolean(new BigDecimal("0"));
            assertEquals(false,aBoolean);
        }
        @DisplayName("test BigDecimal not 0 or 1")
        @Test
        void test7(){
            Boolean aBoolean = CommonUtil.toBoolean(new BigDecimal("2"));
            assertEquals(null,aBoolean);
        }
        @DisplayName("test string true")
        @Test
        void test9(){
            Boolean aBoolean = CommonUtil.toBoolean("true");
            assertEquals(true,aBoolean);
        }
        @DisplayName("test string True")
        @Test
        void test10(){
            Boolean aBoolean = CommonUtil.toBoolean("True");
            assertEquals(true,aBoolean);
        }
        @DisplayName("test string False")
        @Test
        void test11(){
            Boolean aBoolean = CommonUtil.toBoolean("False");
            assertEquals(false,aBoolean);
        }
        @DisplayName("test string 0")
        @Test
        void test12(){
            Boolean aBoolean = CommonUtil.toBoolean("0");
            assertEquals(false,aBoolean);
        }
        @DisplayName("test string 0")
        @Test
        void test13(){
            Boolean aBoolean = CommonUtil.toBoolean("1");
            assertEquals(true,aBoolean);
        }
    }
}
