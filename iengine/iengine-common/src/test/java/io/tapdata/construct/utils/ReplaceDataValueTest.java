package io.tapdata.construct.utils;


import org.junit.jupiter.api.*;

import java.math.BigInteger;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ReplaceDataValueTest {

    /**
     * 测试null
     */
    @Test
    void testNull() {
        Object result = DataUtil.replaceDataValue(null);
        assertTrue(null == result);
    }

    /**
     * 测试一个Map 里面有需要替换的对象
     */
    @Test
    void testMapHasReplaceType() {
        Map<String, Object> map = new HashMap<>();
        BigInteger bigInteger = new BigInteger("9223372036854775807");
        String expectedStringResult = "9223372036854775807";
        map.put("key", bigInteger);
        Object o = DataUtil.replaceDataValue(map);
        assertTrue(o instanceof Map);
        Map<String, Object> resultMap = (Map<String, Object>) o;
        Object value = resultMap.get("key");
        assertTrue(value instanceof String);
        assertEquals(expectedStringResult, value.toString());
    }

    /**
     * 测试一个Map里面没有需要替换的对象
     */
    @Test
    void testMapNoReplaceType() {
        Map<String, Object> map = new HashMap<>();
        String str = "Hello World!";
        map.put("key", str);
        Object o = DataUtil.replaceDataValue(map);
        assertTrue(o instanceof Map);
        Map<String, Object> resultMap = (Map<String, Object>) o;
        Object value = resultMap.get("key");
        assertTrue(value instanceof String);
        assertEquals(str, value.toString());
    }

    /**
     * 测试一个Map里面内嵌一个Map，内嵌Map里面有一需要替换的对象
     */
    @Test
    void testInlineMapHasReplaceType() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> inlineMap = new HashMap<>();
        Instant instant = Instant.ofEpochSecond(1631714400);
        inlineMap.put("key", instant);
        map.put("inlineMap", inlineMap);
        Object resultMap = DataUtil.replaceDataValue(map);
        assertTrue(resultMap instanceof Map);
        Map<String, Object> resultMap1 = (Map<String, Object>) resultMap;
        Object resultInlineMap = resultMap1.get("inlineMap");
        assertTrue(resultInlineMap instanceof Map);
        Map<String, Object> resultInlineMap1 = (Map<String, Object>) resultInlineMap;
        Object key = resultInlineMap1.get("key");
        assertTrue(key instanceof Date);
    }

    /**
     * 测试一个Map里面内嵌一个Map，内嵌Map里面有不需要转换的类型
     */
    @Test
    void testInlineMapNoReplaceType() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> inlineMap = new HashMap<>();
        String str = "tapdata";
        inlineMap.put("key", str);
        map.put("inlineMap", inlineMap);
        Object resultMap = DataUtil.replaceDataValue(map);
        assertTrue(resultMap instanceof Map);
        Map<String, Object> resultMap1 = (Map<String, Object>) resultMap;
        Object resultInlineMap = resultMap1.get("inlineMap");
        assertTrue(resultInlineMap instanceof Map);
        Map<String, Object> resultInlineMap1 = (Map<String, Object>) resultInlineMap;
        Object resultInlineValue = resultInlineMap1.get("key");
        assertTrue(resultInlineValue instanceof String);
        assertEquals(str, resultInlineValue.toString());
    }

    /**
     * 测试一个List里面有有需要替换的对象
     */
    @Test
    void testListHasReplaceType() {
        List list = new ArrayList<>();
        BigInteger bigInteger = new BigInteger("10223372036854775807");
        list.add(bigInteger);
        Object resultList = DataUtil.replaceDataValue(list);
        assertTrue(resultList instanceof List);
        List resultList1 = (List) resultList;
        Object resultBigInteger = resultList1.get(0);
        assertTrue(resultBigInteger instanceof String);
        assertEquals(bigInteger.toString(), (String) resultBigInteger);
    }

    /**
     * 测试一个List里面没有需要替换的对象
     */
    @Test
    void testListNoHasReplaceType() {
        List list = new ArrayList<>();
        String str = "Hello World";
        list.add(str);
        Object resultList = DataUtil.replaceDataValue(list);
        assertTrue(resultList instanceof List);
        List resultList1 = (List) resultList;
        Object resultStr = resultList1.get(0);
        assertTrue(resultStr instanceof String);
        assertEquals(resultStr.toString(), str);
    }

    /**
     * 测试一个List里面有一个内嵌Map
     */
    @Test
    void testListHasMap() {
        List list = new ArrayList();
        Map<String, Object> map = new HashMap<>();
        BigInteger bigInteger = new BigInteger("10223372036854775807");
        map.put("key", bigInteger);
        list.add(map);
        Object resultList = DataUtil.replaceDataValue(list);
        assertTrue(resultList instanceof List);
        List resultList1 = (List) resultList;
        Object resultMap = resultList1.get(0);
        assertTrue(resultMap instanceof Map);
        Map<String, Object> resultMap1 = (Map<String, Object>) resultMap;
        Object mapValue = resultMap1.get("key");
        assertTrue(mapValue instanceof String);
        assertEquals("10223372036854775807", (String) mapValue);
    }

    /**
     * 测试一个List里面有一个内嵌List
     */
    @Test
    void testListHasInlineList() {
        List outerList = new ArrayList();
        List inlineList = new ArrayList();
        outerList.add(inlineList);
        String str = "Hello Tapdata!";
        inlineList.add(str);
        Object outerResultList = DataUtil.replaceDataValue(outerList);
        assertTrue(outerResultList instanceof List);
        List outerResultList1 = (List) outerResultList;
        Object resultInlineList = outerResultList1.get(0);
        assertTrue(resultInlineList instanceof List);
        List resultInlineList1 = (List) resultInlineList;
        Object resultStr = resultInlineList1.get(0);
        assertTrue(resultStr instanceof String);
        assertEquals(str, (String) resultStr);
    }

    /**
     * 测试一个Map里面有一个List
     */
    @Test
    void testMapHasList() {
        Map<String, Object> outerMap = new HashMap<>();
        List inlineList = new ArrayList();
        String str = "Hello Tapdata!";
        outerMap.put("inlineList", inlineList);
        inlineList.add(str);
        Object resultOuterMap = DataUtil.replaceDataValue(outerMap);
        assertTrue(resultOuterMap instanceof Map);
        Map<String, Object> resultOuterMap1 = (Map) resultOuterMap;
        Object inlineList1 = resultOuterMap1.get("inlineList");
        assertTrue(inlineList1 instanceof List);
        List inlineList11 = (List) inlineList1;
        Object resultStr = inlineList11.get(0);
        assertTrue(resultStr instanceof String);
        assertEquals((String) resultStr, str);
    }

}
