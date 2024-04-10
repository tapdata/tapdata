package com.tapdata.constant;

import io.tapdata.flow.engine.V2.filter.FilterUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilterUtilTest {

    @Test
    void testProcessTableFields1() {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "张三");
        data.put("age", 18);
        data.put("id", 1);
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("name");
        fieldNames.add("age");
        Map<String, Object> finalData = FilterUtil.processTableFields(data, fieldNames);
        assertEquals(2, finalData.size());
        assertEquals("张三", finalData.get("name"));
        assertEquals(18, finalData.get("age"));
    }

    @Test
    void testProcessTableFields2() {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "张三");
        data.put("age", 18);
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("name");
        fieldNames.add("age");
        fieldNames.add("id");
        Map<String, Object> finalData = FilterUtil.processTableFields(data, fieldNames);
        assertEquals(2, finalData.size());
        assertEquals("张三", finalData.get("name"));
        assertEquals(18, finalData.get("age"));
    }
}
