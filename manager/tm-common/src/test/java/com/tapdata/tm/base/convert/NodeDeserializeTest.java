package com.tapdata.tm.base.convert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnknownNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.util.JsonUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class NodeDeserializeTest {

    private static Map<String, Object> unknownNode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", "unknown-1");
        node.put("name", "dev only node");
        node.put("type", "dev_only_processor");
        node.put("catalog", "processor");
        node.put("devScript", "return record;");
        node.put("devOptions", Map.of("batch", 10, "mode", "fast"));
        node.put("devFields", List.of("a", "b"));
        return node;
    }

    private static Map<String, Object> tableNode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", "table-1");
        node.put("type", "table");
        node.put("tableName", "orders");
        node.put("connectionId", "conn-1");
        return node;
    }

    private static String dagJson() {
        return JsonUtil.toJsonUseJackson(Map.of(
                "nodes", List.of(tableNode(), unknownNode()),
                "edges", List.of(Map.of("source", "table-1", "target", "unknown-1"))));
    }

    @Test
    @DisplayName("未注册节点类型反序列化为 UnknownNode，保留原 type 与全部原始字段；已注册类型不受影响")
    void shouldKeepUnregisteredNodeAsUnknownNode() {
        Dag dag = JsonUtil.parseJsonUseJackson(dagJson(), Dag.class);

        assertNotNull(dag, "a DAG with an unregistered node type must still be parsed");
        List<Node> nodes = dag.getNodes();
        assertEquals(2, nodes.size());
        TableNode table = assertInstanceOf(TableNode.class, nodes.get(0));
        assertEquals("orders", table.getTableName());
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, nodes.get(1));
        assertEquals("dev_only_processor", unknown.getType());
        assertEquals("unknown-1", unknown.getId());
        assertEquals("dev only node", unknown.getName());
        assertEquals("return record;", unknown.getUnknownProperties().get("devScript"));
        assertEquals(Map.of("batch", 10, "mode", "fast"), unknown.getUnknownProperties().get("devOptions"));
        assertEquals(List.of("a", "b"), unknown.getUnknownProperties().get("devFields"));
    }

    @Test
    @DisplayName("UnknownNode 序列化写回后，原始节点的每个字段与值保持不变")
    void shouldWriteBackUnknownNodeWithoutLosingData() {
        Dag dag = JsonUtil.parseJsonUseJackson(dagJson(), Dag.class);
        assertNotNull(dag);

        Map<String, Object> written = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag),
                new TypeReference<Map<String, Object>>() {
                });
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) written.get("nodes");
        Map<String, Object> writtenUnknown = nodes.get(1);
        unknownNode().forEach((key, value) -> assertEquals(value, writtenUnknown.get(key), "field " + key));
    }

    @Test
    @DisplayName("节点经 Gson 序列化后再解析，保留字段合并回节点而不会层层嵌套")
    void shouldMergeGsonNestedPropertiesBack() {
        Dag dag = JsonUtil.parseJsonUseJackson(dagJson(), Dag.class);
        assertNotNull(dag);
        List<Map<String, Object>> gsonNodes = JsonUtil.parseJsonUseJackson(JsonUtil.toJson(dag.getNodes()),
                new TypeReference<List<Map<String, Object>>>() {
                });

        Dag reparsed = JsonUtil.parseJsonUseJackson(
                JsonUtil.toJsonUseJackson(Map.of("nodes", gsonNodes, "edges", List.of())), Dag.class);

        assertNotNull(reparsed);
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, reparsed.getNodes().get(1));
        assertFalse(unknown.getUnknownProperties().containsKey(UnknownNode.GSON_PRESERVED_PROPERTIES_KEY));
        assertEquals("return record;", unknown.getUnknownProperties().get("devScript"));
        assertEquals(Map.of("batch", 10, "mode", "fast"), unknown.getUnknownProperties().get("devOptions"));
        assertEquals(List.of("a", "b"), unknown.getUnknownProperties().get("devFields"));
    }
}
