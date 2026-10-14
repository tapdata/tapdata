package com.tapdata.tm.inspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectMethod;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TM 侧自定义查询校验契约测试
 * 验证 TM API 层的 custom_query 枚举、customQuery SQL、fieldMapping、sortFields 的序列化和反序列化
 */
public class CustomQueryInspectTmContractTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testCustomQueryMethodValue() throws Exception {
        // InspectMethod is converted to the inspectMethod string field through getValue().
        assertEquals("custom_query", InspectMethod.CUSTOM_QUERY.getValue());

        String json = mapper.valueToTree(InspectMethod.CUSTOM_QUERY.getValue()).toString();
        assertEquals("custom_query", mapper.readValue(json, String.class));
    }

    @Test
    public void testSourceWithCustomQuerySerialization() throws Exception {
        // 验证 TM 侧 Source 的 customQuery 字段序列化
        Source source = new Source();
        source.setConnectionId("test-conn-id");
        source.setConnectionName("test-conn");
        source.setTable("test_table");
        source.setCustomQuery("SELECT SUM(amount) as total, category FROM orders WHERE status = 'completed' GROUP BY category");

        String json = mapper.writeValueAsString(source);
        assertTrue(json.contains("customQuery"), "序列化结果应包含 customQuery 字段");
        assertTrue(json.contains("SELECT SUM"), "序列化结果应包含 SQL 内容");

        Source deserialized = mapper.readValue(json, Source.class);
        assertNotNull(deserialized.getCustomQuery(), "反序列化后 customQuery 不应为 null");
        assertEquals(source.getCustomQuery(), deserialized.getCustomQuery(), "反序列化后 SQL 应一致");
    }

    @Test
    public void testTaskWithFieldMappingAndSortFieldsSerialization() throws Exception {
        // 验证 TM 侧 Task 的 fieldMapping 和 sortFields 字段序列化
        Task task = new Task();

        // 设置字段映射
        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("source_total", "target_total");
        fieldMapping.put("source_category", "target_category");
        task.setFieldMapping(fieldMapping);

        // 设置排序字段
        List<String> sortFields = Arrays.asList("category", "total");
        task.setSortFields(sortFields);

        String json = mapper.writeValueAsString(task);
        assertTrue(json.contains("fieldMapping"), "序列化结果应包含 fieldMapping 字段");
        assertTrue(json.contains("sortFields"), "序列化结果应包含 sortFields 字段");
        assertTrue(json.contains("source_total"), "序列化结果应包含字段映射内容");
        assertTrue(json.contains("category"), "序列化结果应包含排序字段内容");

        Task deserialized = mapper.readValue(json, Task.class);
        assertNotNull(deserialized.getFieldMapping(), "反序列化后 fieldMapping 不应为 null");
        assertNotNull(deserialized.getSortFields(), "反序列化后 sortFields 不应为 null");
        assertEquals(2, deserialized.getFieldMapping().size(), "字段映射应有 2 项");
        assertEquals(2, deserialized.getSortFields().size(), "排序字段应有 2 项");
        assertEquals("target_total", deserialized.getFieldMapping().get("source_total"), "字段映射内容应一致");
        assertTrue(deserialized.getSortFields().contains("category"), "排序字段应包含 category");
    }

    @Test
    public void testTaskWithNullCustomQueryFieldsSerialization() throws Exception {
        // 验证旧任务（没有自定义查询字段）的兼容性
        Task task = new Task();
        task.setFieldMapping(null);
        task.setSortFields(null);

        String json = mapper.writeValueAsString(task);
        Task deserialized = mapper.readValue(json, Task.class);

        // 旧任务的这些字段应为 null，不应抛异常
        assertNull(deserialized.getFieldMapping(), "旧任务的 fieldMapping 应为 null");
        assertNull(deserialized.getSortFields(), "旧任务的 sortFields 应为 null");
    }

    @Test
    public void testCompleteCustomQueryTaskSerialization() throws Exception {
        // 验证完整的自定义查询任务配置的序列化往返
        Task task = new Task();

        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("sum_amount", "sum_amount");
        fieldMapping.put("max_price", "max_price");
        fieldMapping.put("category", "category");
        task.setFieldMapping(fieldMapping);

        List<String> sortFields = Arrays.asList("category");
        task.setSortFields(sortFields);

        String json = mapper.writeValueAsString(task);
        Task deserialized = mapper.readValue(json, Task.class);

        assertEquals(task.getFieldMapping(), deserialized.getFieldMapping(), "完整配置序列化往返后应一致");
        assertEquals(task.getSortFields(), deserialized.getSortFields(), "排序字段序列化往返后应一致");
    }
}
