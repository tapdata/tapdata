package com.tapdata.constant;

import com.tapdata.entity.values.BooleanNotExist;
import io.tapdata.entity.schema.value.DateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
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

    @Nested
    class CompareTest {

        @DisplayName("测试两个null值比较")
        @Test
        void testBothNull() {
            boolean result = CommonUtil.compare(null, null, false, null);
            assertFalse(result, "两个null值应该相等，返回false");
        }

        @DisplayName("测试一个null值比较")
        @Test
        void testOneNull() {
            boolean result1 = CommonUtil.compare(null, "test", false, null);
            assertTrue(result1, "null与非null值应该不相等，返回true");

            boolean result2 = CommonUtil.compare("test", null, false, null);
            assertTrue(result2, "非null与null值应该不相等，返回true");
        }

        @DisplayName("测试相同字符串比较")
        @Test
        void testEqualStrings() {
            boolean result = CommonUtil.compare("hello", "hello", false, null);
            assertFalse(result, "相同字符串应该相等，返回false");
        }

        @DisplayName("测试不同字符串比较")
        @Test
        void testDifferentStrings() {
            boolean result = CommonUtil.compare("hello", "world", false, null);
            assertTrue(result, "不同字符串应该不相等，返回true");
        }

        @DisplayName("测试相同数字比较")
        @Test
        void testEqualNumbers() {
            boolean result1 = CommonUtil.compare(123, 123, false, null);
            assertFalse(result1, "相同整数应该相等，返回false");

            boolean result2 = CommonUtil.compare(123.45, 123.45, false, null);
            assertFalse(result2, "相同小数应该相等，返回false");

            boolean result3 = CommonUtil.compare(new BigDecimal("123.45"), new BigDecimal("123.45"), false, null);
            assertFalse(result3, "相同BigDecimal应该相等，返回false");
        }

        @DisplayName("测试不同数字比较")
        @Test
        void testDifferentNumbers() {
            boolean result1 = CommonUtil.compare(123, 456, false, null);
            assertTrue(result1, "不同整数应该不相等，返回true");

            boolean result2 = CommonUtil.compare(123.45, 678.90, false, null);
            assertTrue(result2, "不同小数应该不相等，返回true");
        }

        @DisplayName("测试布尔值比较")
        @Test
        void testBooleanComparison() {
            boolean result1 = CommonUtil.compare(true, true, false, null);
            assertFalse(result1, "相同布尔值应该相等，返回false");

            boolean result2 = CommonUtil.compare(true, false, false, null);
            assertTrue(result2, "不同布尔值应该不相等，返回true");

            // 测试布尔值与可转换值的比较
            boolean result3 = CommonUtil.compare(true, "1", false, null);
            assertFalse(result3, "true与'1'应该相等，返回false");

            boolean result4 = CommonUtil.compare(false, "0", false, null);
            assertFalse(result4, "false与'0'应该相等，返回false");

            boolean result5 = CommonUtil.compare(true, "true", false, null);
            assertFalse(result5, "true与'true'应该相等，返回false");
        }

        @DisplayName("测试布尔值与不可转换值的比较")
        @Test
        void testBooleanWithNonConvertible() {
            boolean result1 = CommonUtil.compare(true, "2", false, null);
            assertTrue(result1, "true与不可转换的字符串应该不相等，返回true");

            boolean result2 = CommonUtil.compare(false, "invalid", false, null);
            assertTrue(result2, "false与不可转换的字符串应该不相等，返回true");
        }

        @DisplayName("测试Map比较")
        @Test
        void testMapComparison() {
            Map<String, Object> map1 = new HashMap<>();
            map1.put("key1", "value1");
            map1.put("key2", 123);

            Map<String, Object> map2 = new HashMap<>();
            map2.put("key1", "value1");
            map2.put("key2", 123);

            boolean result1 = CommonUtil.compare(map1, map2, false, null);
            assertFalse(result1, "相同内容的Map应该相等，返回false");

            Map<String, Object> map3 = new HashMap<>();
            map3.put("key1", "value1");
            map3.put("key2", 456);

            boolean result2 = CommonUtil.compare(map1, map3, false, null);
            assertTrue(result2, "不同内容的Map应该不相等，返回true");

            Map<String, Object> map4 = new HashMap<>();
            map4.put("key1", "value1");

            boolean result3 = CommonUtil.compare(map1, map4, false, null);
            assertTrue(result3, "不同大小的Map应该不相等，返回true");
        }

        @DisplayName("测试Collection比较")
        @Test
        void testCollectionComparison() {
            List<Object> list1 = Arrays.asList("a", "b", "c");
            List<Object> list2 = Arrays.asList("a", "b", "c");

            boolean result1 = CommonUtil.compare(list1, list2, false, null);
            assertFalse(result1, "相同内容的List应该相等，返回false");

            List<Object> list3 = Arrays.asList("a", "b", "d");
            boolean result2 = CommonUtil.compare(list1, list3, false, null);
            assertTrue(result2, "不同内容的List应该不相等，返回true");

            List<Object> list4 = Arrays.asList("a", "b");
            boolean result3 = CommonUtil.compare(list1, list4, false, null);
            assertTrue(result3, "不同大小的List应该不相等，返回true");

            Set<Object> set1 = new HashSet<>(Arrays.asList("x", "y", "z"));
            Set<Object> set2 = new HashSet<>(Arrays.asList("x", "y", "z"));
            boolean result4 = CommonUtil.compare(set1, set2, false, null);
            assertFalse(result4, "相同内容的Set应该相等，返回false");
        }

        @DisplayName("测试DateTime比较 - 不忽略时间精度")
        @Test
        void testDateTimeComparisonWithoutIgnorePrecision() {
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");

            DateTime dateTime1 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.123456789", formatter1));
            DateTime dateTime2 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter2));

            boolean result = CommonUtil.compare(dateTime1, dateTime2, false, null);
            assertTrue(result, "不同精度的DateTime在不忽略精度时应该不相等，返回true");
        }

        @DisplayName("测试DateTime比较 - 忽略时间精度，四舍五入模式")
        @Test
        void testDateTimeComparisonWithIgnorePrecisionRoundUp() {
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");

            DateTime dateTime1 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.123456789", formatter1));
            DateTime dateTime2 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12346", formatter2));

            boolean result = CommonUtil.compare(dateTime1, dateTime2, true, "HALF_UP");
            assertFalse(result, "四舍五入后相等的DateTime应该相等，返回false");
        }

        @DisplayName("测试DateTime比较 - 忽略时间精度，截断模式")
        @Test
        void testDateTimeComparisonWithIgnorePrecisionTruncate() {
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");

            DateTime dateTime1 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.123456789", formatter1));
            DateTime dateTime2 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter2));

            boolean result = CommonUtil.compare(dateTime1, dateTime2, true, "DOWN");
            assertFalse(result, "截断后相等的DateTime应该相等，返回false");
        }

        @DisplayName("测试DateTime比较 - 相同精度")
        @Test
        void testDateTimeComparisonSamePrecision() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");

            DateTime dateTime1 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter));
            DateTime dateTime2 = new DateTime(LocalDateTime.parse("2023-05-15 14:30:25.12345", formatter));

            boolean result1 = CommonUtil.compare(dateTime1, dateTime2, true, "HALF_UP");
            assertFalse(result1, "相同的DateTime应该相等，返回false");

            boolean result2 = CommonUtil.compare(dateTime1, dateTime2, false, null);
            assertFalse(result2, "相同的DateTime应该相等，返回false");
        }

        @DisplayName("测试Instant比较")
        @Test
        void testInstantComparison() {
            Instant instant1 = Instant.parse("2023-05-15T14:30:25.123456789Z");
            Instant instant2 = Instant.parse("2023-05-15T14:30:25.123456789Z");

            boolean result1 = CommonUtil.compare(instant1, instant2, false, null);
            assertFalse(result1, "相同的Instant应该相等，返回false");

            Instant instant3 = Instant.parse("2023-05-15T14:30:25.123456790Z");
            boolean result2 = CommonUtil.compare(instant1, instant3, false, null);
            assertTrue(result2, "不同的Instant应该不相等，返回true");
        }

        @DisplayName("测试混合类型比较")
        @Test
        void testMixedTypeComparison() {
            // 字符串与数字比较
            boolean result1 = CommonUtil.compare("123", 123, false, null);
            assertFalse(result1, "字符串'123'与数字123应该相等，返回false");

            boolean result2 = CommonUtil.compare("123.45", 123.45, false, null);
            assertFalse(result2, "字符串'123.45'与数字123.45应该相等，返回false");

            boolean result3 = CommonUtil.compare("abc", 123, false, null);
            assertTrue(result3, "字符串'abc'与数字123应该不相等，返回true");
        }



        @DisplayName("测试BigDecimal精度比较")
        @Test
        void testBigDecimalPrecisionComparison() {
            BigDecimal bd1 = new BigDecimal("123.4500");
            BigDecimal bd2 = new BigDecimal("123.45");

            boolean result1 = CommonUtil.compare(bd1, bd2, false, null);
            assertFalse(result1, "数值相等的BigDecimal应该相等，返回false");

            BigDecimal bd3 = new BigDecimal("123.4501");
            boolean result2 = CommonUtil.compare(bd1, bd3, false, null);
            assertTrue(result2, "数值不等的BigDecimal应该不相等，返回true");
        }

        @DisplayName("测试异常情况处理")
        @Test
        void testExceptionHandling() {
            // 测试不可比较的对象
            Object obj1 = new Object();
            Object obj2 = new Object();

            boolean result = CommonUtil.compare(obj1, obj2, false, null);
            assertTrue(result, "不同的Object实例应该不相等，返回true");

            // 测试相同的Object实例
            boolean result2 = CommonUtil.compare(obj1, obj1, false, null);
            assertFalse(result2, "相同的Object实例应该相等，返回false");
        }

        @DisplayName("测试嵌套集合比较")
        @Test
        void testNestedCollectionComparison() {
            List<Object> innerList1 = Arrays.asList("a", "b");
            List<Object> innerList2 = Arrays.asList("a", "b");
            List<Object> outerList1 = Arrays.asList(innerList1, "c");
            List<Object> outerList2 = Arrays.asList(innerList2, "c");

            boolean result1 = CommonUtil.compare(outerList1, outerList2, false, null);
            assertFalse(result1, "相同内容的嵌套List应该相等，返回false");

            List<Object> innerList3 = Arrays.asList("a", "d");
            List<Object> outerList3 = Arrays.asList(innerList3, "c");

            boolean result2 = CommonUtil.compare(outerList1, outerList3, false, null);
            assertTrue(result2, "不同内容的嵌套List应该不相等，返回true");
        }
    }
}
