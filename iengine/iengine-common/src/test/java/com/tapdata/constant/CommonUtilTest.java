package com.tapdata.constant;

import com.tapdata.entity.values.BooleanNotExist;
import io.tapdata.entity.schema.value.DateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class CommonUtilTest {
    @Nested
    class compareBooleanTest{
        @DisplayName("test one value is boolean")
        @Test
        void test1(){
            boolean result = CommonUtil.compareBoolean(true, null);
            assertEquals(false,result);
        }
        @DisplayName("test one value is boolean")
        @Test
        void test5(){
            boolean result = CommonUtil.compareBoolean(null, true);
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
            Object aBoolean = CommonUtil.toBoolean(2);
            assertEquals(true, aBoolean instanceof BooleanNotExist);
        }
        @DisplayName("test Number 1")
        @Test
        void test2(){
            Object aBoolean = CommonUtil.toBoolean(1);
            assertEquals(true,aBoolean);
        }
        @DisplayName("test Number 0")
        @Test
        void test3(){
            Object aBoolean = CommonUtil.toBoolean(0);
            assertEquals(false,aBoolean);
        }
        @DisplayName("test Number 0.0")
        @Test
        void test4(){
            Object aBoolean = CommonUtil.toBoolean(0.0);
            assertEquals(false,aBoolean);
        }
        @DisplayName("test BigDecimal 1")
        @Test
        void test5(){
            Object aBoolean = CommonUtil.toBoolean(new BigDecimal("1"));
            assertEquals(true,aBoolean);
        }
        @DisplayName("test BigDecimal 0")
        @Test
        void test6(){
            Object aBoolean = CommonUtil.toBoolean(new BigDecimal("0"));
            assertEquals(false,aBoolean);
        }
        @DisplayName("test BigDecimal not 0 or 1")
        @Test
        void test7(){
            Object aBoolean = CommonUtil.toBoolean(new BigDecimal("2"));
            assertEquals(true,aBoolean instanceof BooleanNotExist);
        }
        @DisplayName("test string true")
        @Test
        void test9(){
            Object aBoolean = CommonUtil.toBoolean("true");
            assertEquals(true,aBoolean);
        }
        @DisplayName("test string True")
        @Test
        void test10(){
            Object aBoolean = CommonUtil.toBoolean("True");
            assertEquals(true,aBoolean);
        }
        @DisplayName("test string False")
        @Test
        void test11(){
            Object aBoolean = CommonUtil.toBoolean("False");
            assertEquals(false,aBoolean);
        }
        @DisplayName("test string 0")
        @Test
        void test12(){
            Object aBoolean = CommonUtil.toBoolean("0");
            assertEquals(false,aBoolean);
        }
        @DisplayName("test string 0")
        @Test
        void test13(){
            Object aBoolean = CommonUtil.toBoolean("1");
            assertEquals(true,aBoolean);
        }
        @DisplayName("test string not 0,false,1,true")
        @Test
        void test14(){
            Object aBoolean = CommonUtil.toBoolean("2");
            assertEquals(true,aBoolean instanceof BooleanNotExist);
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
            int result = CommonUtil.compareObjects(val1, val2,false,null);
            assertEquals(0, result);
        }
        @DisplayName("test val1 is boolean and val2 can not cast to boolean")
        @Test
        void test2(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = true;
            val2[0] = "2";
            int result = CommonUtil.compareObjects(val1, val2,false,null);
            assertEquals(1, result);
        }
        @DisplayName("test val2 is boolean and val1 can not cast to boolean")
        @Test
        void test3(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = 2;
            val2[0] = true;
            int result = CommonUtil.compareObjects(val1, val2,false,null);
            assertEquals(-1, result);
        }
        @DisplayName("test val2 is boolean and val1 can not cast to boolean")
        @Test
        void test4(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = "1";
            val2[0] = true;
            int result = CommonUtil.compareObjects(val1, val2,false,null);
            assertEquals(0, result);
        }

        @DisplayName("test val1,val2 is byte")
        @Test
        void test5(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            val1[0] = "test".getBytes(StandardCharsets.UTF_8);
            val2[0] = "test".getBytes(StandardCharsets.UTF_8);
            int result = CommonUtil.compareObjects(val1, val2,false,null);
            assertEquals(0, result);
        }
    }

    @Nested
    class compareDateTimeTest{
        @DisplayName("测试val1 与val2 精度不一样，val2 精度被四十五入")
        @Test
        void test11(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");
            val1[0] = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.123456789", formatter));
            val2[0] =new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12346", formatter2));
            assertEquals(0, CommonUtil.compareObjects(val1,val2,true,"HALF_UP"));
            assertNotEquals(0,CommonUtil.compareObjects(val1,val2,false,null));
        }
        @DisplayName("测试val1 与val2 精度不一样，val2 精度直接截断")
        @Test
        void test12(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");
            val1[0] = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.123456789", formatter));
            val2[0] =new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter2));
            assertEquals(0, CommonUtil.compareObjects(val1,val2,true,"DOWN"));
            assertNotEquals(0,CommonUtil.compareObjects(val1,val2,false,null));
        }
        @DisplayName("测试val1 与val2 精度一样，比较相等值")
        @Test
        void test13(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");
            val1[0] = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter));
            val2[0] =new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter2));
            assertEquals(0, CommonUtil.compareObjects(val1,val2,true,"HALF_UP"));
            assertEquals(0,CommonUtil.compareObjects(val1,val2,false,null));
        }
        @DisplayName("测试val1 与val2 精度一样，但val2不能保存10整位，只能保存1/3位")
        @Test
        void test14(){
            Object[] val1 = new Object[10];
            Object[] val2 = new Object[10];
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            val1[0] = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.122", formatter));
            val2[0] =new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.120", formatter2));
            assertEquals(0, CommonUtil.compareObjects(val1,val2,true,"HALF_UP"));
            assertNotEquals(0,CommonUtil.compareObjects(val1,val2,false,null));
        }


    }
}
