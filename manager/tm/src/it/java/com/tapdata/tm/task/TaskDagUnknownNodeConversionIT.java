package com.tapdata.tm.task;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnknownNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.config.DefaultMongoConfig;
import com.tapdata.tm.task.entity.TaskEntity;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Task documents are read and written through the production Mongo conversions (DagDeserializeConvert /
 * DagSerializeConvert with the tm-common node deserializer). Storage itself is the external boundary.
 */
@Timeout(30)
class TaskDagUnknownNodeConversionIT {

    private static MappingMongoConverter productionConverter() {
        MongoCustomConversions conversions = new DefaultMongoConfig().mongoCustomConversions();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        mappingContext.afterPropertiesSet();
        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        when(databaseFactory.getExceptionTranslator()).thenReturn(mock(PersistenceExceptionTranslator.class));
        MappingMongoConverter converter = new MappingMongoConverter(new DefaultDbRefResolver(databaseFactory), mappingContext);
        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();
        return converter;
    }

    private static Document unknownNode() {
        return new Document("id", "unknown-1")
                .append("name", "dev only node")
                .append("type", "dev_only_processor")
                .append("catalog", "processor")
                .append("devScript", "return record;")
                .append("devOptions", new Document("batch", 10).append("mode", "fast"))
                .append("devFields", List.of("a", "b"));
    }

    /** A node of a newer build whose known field holds a value this version cannot bind (an unknown catalog enum). */
    private static Document unbindableUnknownNode() {
        return new Document("id", "unknown-1")
                .append("name", "dev ai node")
                .append("type", "dev_ai_processor")
                .append("catalog", "aiProcessor")
                .append("isTransformed", true)
                .append("devScript", "return record;")
                .append("devOptions", new Document("batch", 10));
    }

    private static Document taskDocument() {
        return taskDocument(unknownNode());
    }

    private static Document taskDocument(Document unknownNode) {
        Document dag = new Document("nodes", List.of(
                new Document("id", "table-1").append("type", "table").append("tableName", "orders")
                        .append("connectionId", "conn-1"),
                unknownNode))
                .append("edges", List.of(new Document("source", "table-1").append("target", "unknown-1")));
        return new Document("_id", new ObjectId()).append("name", "task left by a dev build").append("dag", dag);
    }

    @Test
    @DisplayName("L2：含开发版遗留未知节点类型的任务文档可经生产 Mongo 转换读取，写回后原节点数据不丢失")
    void shouldReadAndWriteBackTaskWithUnknownNode() {
        MappingMongoConverter converter = productionConverter();

        TaskEntity task = converter.read(TaskEntity.class, taskDocument());

        List<Node> nodes = task.getDag().getNodes();
        assertEquals(2, nodes.size());
        assertInstanceOf(TableNode.class, task.getDag().getNode("table-1"));
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, task.getDag().getNode("unknown-1"));
        assertEquals("dev_only_processor", unknown.getType());
        assertEquals(1, task.getDag().getEdges().size());

        Document written = new Document();
        converter.write(task, written);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> writtenNodes = (List<Map<String, Object>>) ((Map<String, Object>) written.get("dag")).get("nodes");
        Map<String, Object> writtenUnknown = writtenNodes.stream()
                .filter(node -> "unknown-1".equals(node.get("id"))).findFirst().orElseThrow();
        unknownNode().forEach((key, value) -> assertEquals(value, writtenUnknown.get(key), "field " + key));
    }

    @Test
    @DisplayName("L2：未知节点含当前版本无法绑定的已知字段值（未知 catalog 枚举）时，任务文档仍可读取且不丢节点，写回原值不变")
    void shouldKeepUnknownNodeWhoseFieldsCannotBeBound() {
        MappingMongoConverter converter = productionConverter();

        TaskEntity task = converter.read(TaskEntity.class, taskDocument(unbindableUnknownNode()));

        assertEquals(2, task.getDag().getNodes().size(), "the node must not be dropped");
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, task.getDag().getNode("unknown-1"));
        assertEquals("dev_ai_processor", unknown.getType());
        assertEquals(1, task.getDag().getEdges().size());

        Document written = new Document();
        converter.write(task, written);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> writtenNodes = (List<Map<String, Object>>) ((Map<String, Object>) written.get("dag")).get("nodes");
        Map<String, Object> writtenUnknown = writtenNodes.stream()
                .filter(node -> "unknown-1".equals(node.get("id"))).findFirst().orElseThrow();
        unbindableUnknownNode().forEach((key, value) -> assertEquals(value, writtenUnknown.get(key), "field " + key));
    }
}
