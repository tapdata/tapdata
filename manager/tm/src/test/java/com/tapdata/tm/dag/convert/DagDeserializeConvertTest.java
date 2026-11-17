package com.tapdata.tm.dag.convert;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.UnknownNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DagDeserializeConvertTest {

    @Test
    @DisplayName("含未注册节点类型的 DAG Document 转换成功，节点与边完整")
    void shouldConvertDagContainingUnknownNode() {
        Document dag = new Document("nodes", List.of(
                new Document("id", "table-1").append("type", "table").append("tableName", "orders"),
                new Document("id", "unknown-1").append("type", "dev_only_processor").append("devScript", "x")))
                .append("edges", List.of(new Document("source", "table-1").append("target", "unknown-1")));

        DAG result = new DagDeserializeConvert().convert(dag);

        assertEquals(2, result.getNodes().size());
        assertInstanceOf(TableNode.class, result.getNode("table-1"));
        UnknownNode unknown = assertInstanceOf(UnknownNode.class, result.getNode("unknown-1"));
        assertEquals("dev_only_processor", unknown.getType());
        List<Edge> edges = result.getEdges();
        assertEquals(1, edges.size());
        assertEquals("table-1", edges.get(0).getSource());
        assertEquals("unknown-1", edges.get(0).getTarget());
    }

    @Test
    @DisplayName("DAG 无法解析时抛出说明原因的异常，而不是 NullPointerException 或空 DAG")
    void shouldFailWithClearMessageWhenDagCannotBeParsed() {
        Document broken = new Document("nodes", "not-a-node-list");

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new DagDeserializeConvert().convert(broken));
        assertTrue(error.getMessage().contains("Unable to deserialize task DAG"));
    }
}
