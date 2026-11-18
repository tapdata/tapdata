package com.tapdata.tm.base.convert;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnknownNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.util.JsonUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Edge cases of keeping and writing back nodes whose type is not registered in this version. */
class UnknownNodeRoundTripTest {

    private static Map<String, Object> node(String id, String type, Object... properties) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", id);
        node.put("name", "node " + id);
        node.put("type", type);
        for (int i = 0; i < properties.length; i += 2) {
            node.put((String) properties[i], properties[i + 1]);
        }
        return node;
    }

    private static String dagJson(Map<String, Object> unknownNode) {
        Map<String, Object> table = node("table-1", "table", "tableName", "orders");
        return JsonUtil.toJsonUseJackson(Map.of("nodes", List.of(table, unknownNode),
                "edges", List.of(Map.of("source", "table-1", "target", unknownNode.get("id")))));
    }

    /** Writes the DAG the way it is persisted (Jackson with {@link NodeSerialize}) and returns the written node. */
    private static Map<String, Object> writtenNode(Dag dag, String id) {
        Map<String, Object> written = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag),
                new TypeReference<Map<String, Object>>() {
                });
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) written.get("nodes");
        return nodes.stream().filter(n -> id.equals(n.get("id"))).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("未知节点含当前版本无法绑定的字段值（未知 catalog 枚举）时，以原始字段构造 UnknownNode，不丢节点，写回原值不变")
    void shouldKeepRawPropertiesWhenBindingFails() {
        Map<String, Object> original = node("unknown-1", "dev_ai_processor",
                "catalog", "aiProcessor", "devScript", "return record;", "devOptions", Map.of("batch", 10));

        Dag dag = JsonUtil.parseJsonUseJackson(dagJson(original), Dag.class);

        assertNotNull(dag);
        assertEquals(2, dag.getNodes().size(), "the node must not be dropped");
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, dag.getNodes().get(1));
        assertEquals("unknown-1", unknown.getId());
        assertEquals("dev_ai_processor", unknown.getType());
        assertEquals("node unknown-1", unknown.getName());
        assertEquals("aiProcessor", unknown.getUnknownProperties().get("catalog"));

        Map<String, Object> written = writtenNode(dag, "unknown-1");
        original.forEach((key, value) -> assertEquals(value, written.get(key), "field " + key));
        assertFalse(written.containsKey(UnknownNode.GSON_PRESERVED_PROPERTIES_KEY));
    }

    @Test
    @DisplayName("写回时保留的原值覆盖 Gson 按字段写出的同名 key（isTransformed）")
    void shouldWriteBackKeptValueOverGsonFieldValue() {
        Map<String, Object> original = node("unknown-1", "dev_only_processor", "isTransformed", true);

        Dag dag = JsonUtil.parseJsonUseJackson(dagJson(original), Dag.class);

        assertNotNull(dag);
        assertEquals(true, writtenNode(dag, "unknown-1").get("isTransformed"));
    }

    @Test
    @DisplayName("业务字段本身名为 preservedProperties 时，经 Jackson 写回和 Gson 往返后结构都不变")
    void shouldKeepBusinessPropertyNamedPreservedProperties() {
        Map<String, Object> original = node("unknown-1", "dev_only_processor",
                "preservedProperties", Map.of("a", 1), "devScript", "return record;");

        Dag dag = JsonUtil.parseJsonUseJackson(dagJson(original), Dag.class);
        assertNotNull(dag);
        assertEquals(Map.of("a", 1), writtenNode(dag, "unknown-1").get("preservedProperties"));

        List<Map<String, Object>> gsonNodes = JsonUtil.parseJsonUseJackson(JsonUtil.toJson(dag.getNodes()),
                new TypeReference<List<Map<String, Object>>>() {
                });
        Dag reparsed = JsonUtil.parseJsonUseJackson(
                JsonUtil.toJsonUseJackson(Map.of("nodes", gsonNodes, "edges", List.of())), Dag.class);
        assertNotNull(reparsed);
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, reparsed.getNodes().get(1));
        assertEquals(Map.of("a", 1), unknown.getUnknownProperties().get("preservedProperties"));
        assertEquals("return record;", unknown.getUnknownProperties().get("devScript"));
        assertFalse(unknown.getUnknownProperties().containsKey("a"));
    }

    @Test
    @DisplayName("同一未知 type 只在首次出现时打 WARN，之后降为 DEBUG")
    void shouldWarnOncePerUnknownType() {
        Logger logger = (Logger) LoggerFactory.getLogger(NodeDeserialize.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        try {
            String json = dagJson(node("unknown-1", "dev_warn_once_processor"));
            JsonUtil.parseJsonUseJackson(json, Dag.class);
            JsonUtil.parseJsonUseJackson(json, Dag.class);
            JsonUtil.parseJsonUseJackson(json, Dag.class);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }

        List<ILoggingEvent> events = appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains("dev_warn_once_processor")).toList();
        assertEquals(1, events.stream().filter(e -> e.getLevel() == Level.WARN).count());
        assertEquals(2, events.stream().filter(e -> e.getLevel() == Level.DEBUG).count());
    }

    @Test
    @DisplayName("节点注册表为空时直接报注册失败，不把已知类型兜底成 UnknownNode")
    void shouldFailWhenNodeRegistryIsEmpty() {
        Map<String, Class<? extends Node>> registered = new HashMap<>(DAG.nodeMapping);
        DAG.nodeMapping.clear();
        try {
            JsonMappingException error = assertThrows(JsonMappingException.class,
                    () -> new ObjectMapper().readValue(dagJson(node("unknown-1", "dev_only_processor")), Dag.class));
            IllegalStateException cause = assertInstanceOf(IllegalStateException.class, error.getCause());
            assertTrue(cause.getMessage().contains("registration failed"), cause.getMessage());
        } finally {
            DAG.nodeMapping.putAll(registered);
        }
    }

    @Test
    @DisplayName("节点类加载失败的 type 报注册失败，不当作当前版本不支持的节点")
    void shouldFailWhenNodeClassFailedToLoad() throws Exception {
        Field field = DAG.class.getDeclaredField("failedNodeTypes");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<String> failedNodeTypes = (Set<String>) field.get(null);
        failedNodeTypes.add("dev_failed_to_load");
        try {
            assertTrue(DAG.isNodeTypeRegistrationFailed("dev_failed_to_load"));
            assertFalse(DAG.isNodeTypeRegistrationFailed("dev_only_processor"));
            JsonMappingException error = assertThrows(JsonMappingException.class,
                    () -> new ObjectMapper().readValue(dagJson(node("unknown-1", "dev_failed_to_load")), Dag.class));
            IllegalStateException cause = assertInstanceOf(IllegalStateException.class, error.getCause());
            assertTrue(cause.getMessage().contains("dev_failed_to_load"), cause.getMessage());
        } finally {
            failedNodeTypes.remove("dev_failed_to_load");
        }
    }
}
