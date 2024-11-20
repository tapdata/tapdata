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
        @DisplayName("test string not 0,false,1,true")
        @Test
        void test14(){
            Boolean aBoolean = CommonUtil.toBoolean("2");
            assertEquals(null,aBoolean);
        }
    }
    @Nested
    class compareObjectsTest{
        @DisplayName("test val1 is boolean and val2 can cast to boolean")
        @Test
        void test1() {
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = true;
            val2[0] = "1";
            int result = CommonUtil.compareObjects(val1, val2);
            assertEquals(0, result);
        }
        @DisplayName("test val1 is boolean and val2 can not cast to boolean")
        @Test
        void test2(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = true;
            val2[0] = "2";
            int result = CommonUtil.compareObjects(val1, val2);
            assertEquals(1, result);
        }
        @DisplayName("test val2 is boolean and val1 can not cast to boolean")
        @Test
        void test3(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = 2;
            val2[0] = true;
            int result = CommonUtil.compareObjects(val1, val2);
            assertEquals(-1, result);
        }
    }
}
