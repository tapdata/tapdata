package io.tapdata.flow.engine.V2.filter;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilterUtilTest {

    @Test
    public void processTableFields() {
        Map<String, Object> eventData = new HashMap<>();
        final String key1 = "field_char";
        final String key2 = "field_int";
        final String key3 = "field_double";
        final int value1 = 1;
        final char value2 = 'e';
        final double value3 = 12.00D;
        eventData.put(key1, value1);
        eventData.put(key2, value2);
        eventData.put(key3, value3);
        Set<String> fields = new HashSet<>();
        fields.add(key1);
        fields.add(key2);
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertEquals(fields.size(), eventData.size());
        Assert.assertTrue(eventData.containsKey(key1));
        Assert.assertTrue(eventData.containsKey(key2));
        Assert.assertFalse(eventData.containsKey(key3));
        Assert.assertEquals(eventData.get(key1), value1);
        Assert.assertEquals(eventData.get(key2), value2);
    }

    @Test
    public void processTableFieldsOfDeleteField() {
        Map<String, Object> eventData = new HashMap<>();
        final String key1 = "field_char";
        final String key2 = "field_int";
        final String key3 = "field_double";
        final int value1 = 1;
        final char value2 = 'e';
        final double value3 = 12.00D;
        eventData.put(key1, value1);
        eventData.put(key2, value2);
        Set<String> fields = new HashSet<>();
        fields.add(key1);
        fields.add(key2);
        fields.add(key3);
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertTrue(fields.size() > eventData.size());
        Assert.assertTrue(eventData.containsKey(key1));
        Assert.assertTrue(eventData.containsKey(key2));
        Assert.assertFalse(eventData.containsKey(key3));
        Assert.assertEquals(eventData.get(key1), value1);
        Assert.assertEquals(eventData.get(key2), value2);
    }

    @Test
    public void processTableFieldsOfNullEventData() {
        Map<String, Object> eventData = null;
        final String key1 = "field_char";
        Set<String> fields = new HashSet<>();
        fields.add(key1);
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertNull(eventData);

    }

    @Test
    public void processTableFieldsOfEmptyEventData() {
        Map<String, Object> eventData = new HashMap<>();
        final String key1 = "field_char";
        Set<String> fields = new HashSet<>();
        fields.add(key1);
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertTrue(fields.size() > eventData.size());
        Assert.assertFalse(eventData.containsKey(key1));
    }

    @Test
    public void processTableFieldsOfNullField() {
        Map<String, Object> eventData = new HashMap<>();
        final String key1 = "field_char";
        final char value1 = 'd';
        eventData.put(key1, value1);
        Set<String> fields = null;
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertEquals(1, eventData.size());
    }

    @Test
    public void processTableFieldsOfEmptyField() {
        Map<String, Object> eventData = new HashMap<>();
        final String key1 = "field_char";
        final char value1 = 'd';
        eventData.put(key1, value1);
        Set<String> fields = new HashSet<>();
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertEquals(1, eventData.size());
        Assert.assertTrue(eventData.containsKey(key1));
    }

    @Test
    public void processTableFieldsOfEventDataContainsFieldsNotContain() {
        Map<String, Object> eventData = new HashMap<>();
        final String key1 = "field_char";
        final String key2 = "field_int";
        final char value1 = 'd';
        eventData.put(key1, value1);
        Set<String> fields = new HashSet<>();
        fields.add(key2);
        eventData = FilterUtil.processTableFields(eventData, fields);
        Assert.assertEquals(0, eventData.size());
        Assert.assertFalse(eventData.containsKey(key1));
        Assert.assertFalse(eventData.containsKey(key2));
    }

    @Test
    public void testProcessTableFields1() {
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
    public void testProcessTableFields2() {
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
