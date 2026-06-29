package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AffectedKeyCalculatorTest {
    @Test
    void constructor_validatesRequiredArguments() {
        DuckDbOperator operator = mock(DuckDbOperator.class);

        assertThrows(IllegalArgumentException.class, () -> new AffectedKeyCalculator(
                List.of(),
                "main",
                List.of(),
                Map.of(),
                operator,
                Map.of(),
                "SELECT 1"
        ));

        assertThrows(NullPointerException.class, () -> new AffectedKeyCalculator(
                List.of("id"),
                "main",
                List.of(),
                Map.of(),
                null,
                Map.of(),
                "SELECT 1"
        ));

        assertThrows(IllegalArgumentException.class, () -> new AffectedKeyCalculator(
                List.of("id"),
                "main",
                List.of(),
                Map.of(),
                operator,
                Map.of(),
                "   "
        ));
    }

    @Test
    void calculateAffectedBeforeKeys_mainTable_fastPath() throws SQLException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("id"),
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("users", schema("node1", "users")),
                "SELECT id FROM users"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.getMainTableBeforePks().add(Map.of("id", 1L));
        List<Map<String, Object>> beforeKeys = calculator.calculateAffectedBeforeKeys(List.of(r), "users");
        assertEquals(List.of(Map.of("id", 1L)), beforeKeys);
    }

    @Test
    void calculateAffectedAfterKeys_mainTable_returnsPkListAndQueryResults() throws SQLException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        when(operator.executeQuery(anyString())).thenReturn(List.of(
                Map.of("id", 1L, "name", "A")
        ));

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("id"),
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("users", schema("node1", "users")),
                "SELECT id, name FROM users"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.getMainTableAfterPks().add(Map.of("id", 1L));
        r.setAfterRow("id=1", Map.of("id", 1L, "name", "A"));

        AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(List.of(r), "users");
        assertFalse(result.isEmpty());
        assertEquals(List.of(Map.of("id", 1L)), result.getWideTablePks());
        assertEquals(1, result.getWideTableQueryResults().size());
        assertEquals(1, result.getAfterRows().size());
    }

    @Test
    void calculateAffectedAfterKeys_subTable_usesCteAndReturnsPks() throws SQLException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        AtomicInteger queries = new AtomicInteger();
        doAnswer(invocation -> {
            queries.incrementAndGet();
            return List.of(
                    Map.of("id", 1L, "v", "x"),
                    Map.of("id", 2L, "v", "y")
            );
        }).when(operator).executeQuery(anyString());

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("id"),
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("orders", schema("node_orders", "orders")),
                "SELECT id, v FROM orders"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.setAfterRow("id=1", Map.of("id", 1L, "v", "x"));

        AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(List.of(r), "orders");
        assertEquals(1, queries.get());
        assertEquals(2, result.getWideTablePks().size());
        assertEquals(2, result.getWideTableQueryResults().size());
    }

    private static NodeSchemaInfo schema(String nodeId, String tableName) {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        TapField id = new TapField();
        id.setName("id");
        id.setOriginalFieldName("id");
        id.setDataType("BIGINT");
        id.setTapType(new TapNumber());
        fields.put("id", id);
        TapField name = new TapField();
        name.setName("name");
        name.setOriginalFieldName("name");
        name.setDataType("VARCHAR");
        fields.put("name", name);
        tapTable.setNameFieldMap(fields);
        return new NodeSchemaInfo(nodeId, tableName, tableName, List.of("id"), fields, tapTable, null);
    }
}

