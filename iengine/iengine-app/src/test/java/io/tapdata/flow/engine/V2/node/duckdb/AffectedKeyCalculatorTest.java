package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AffectedKeyCalculatorTest {
    @Test
    void constructor_validatesRequiredArguments() {
        DuckDbOperator operator = queryStub().operator;

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
    void calculateAffectedBeforeKeys_mainTable_usesCteWideTablePks() throws SQLException {
        QueryStub stub = queryStub(List.of(
                Map.of("wide_id", 1L, "name", "A")
        ));
        DuckDbOperator operator = stub.operator;

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("wide_id"),
                "wide_users",
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("users", schema("node1", "users")),
                "SELECT id AS wide_id, name FROM users"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.getMainTableBeforePks().add(Map.of("id", 1L));
        r.getBeforeRows().add(Map.of("id", 1L, "name", "A"));
        List<Map<String, Object>> beforeKeys = calculator.calculateAffectedBeforeKeys(List.of(r), "users");
        assertEquals(List.of(Map.of("wide_id", 1L)), beforeKeys);
    }

    @Test
    void calculateAffectedBeforeKeys_mainTableFallsBackToExistingWideRowsWhenJoinNoLongerMatches() throws SQLException {
        QueryStub stub = queryStub(
                List.of(),
                List.of(
                    Map.of("wide_id", 1L),
                    Map.of("wide_id", 2L)
                )
        );
        DuckDbOperator operator = stub.operator;

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("wide_id"),
                "wide_users",
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("users", schema("node1", "users")),
                "SELECT u.id AS wide_id, u.name FROM users u JOIN orders o ON u.id = o.user_id"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.getBeforeRows().add(Map.of("id", 1L, "name", "A"));

        List<Map<String, Object>> beforeKeys = calculator.calculateAffectedBeforeKeys(List.of(r), "users");
        assertEquals(2, stub.queries.get());
        assertEquals(List.of(Map.of("wide_id", 1L), Map.of("wide_id", 2L)), beforeKeys);
    }

    @Test
    void calculateAffectedBeforeKeys_mainTableMergesExistingWideRowsWhenCtePartiallyMatches() throws SQLException {
        QueryStub stub = queryStub(
                List.of(
                        Map.of("wide_id", 1L)
                ),
                List.of(
                    Map.of("wide_id", 1L),
                    Map.of("wide_id", 2L)
                )
        );
        DuckDbOperator operator = stub.operator;

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("wide_id"),
                "wide_users",
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("users", schema("node1", "users")),
                "SELECT u.id AS wide_id, u.name FROM users u JOIN orders o ON u.id = o.user_id"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.getBeforeRows().add(Map.of("id", 1L, "name", "A"));

        List<Map<String, Object>> beforeKeys = calculator.calculateAffectedBeforeKeys(List.of(r), "users");
        assertEquals(2, stub.queries.get());
        assertEquals(List.of(Map.of("wide_id", 1L), Map.of("wide_id", 2L)), beforeKeys);
    }

    @Test
    void calculateAffectedAfterKeys_mainTable_returnsPkListAndQueryResults() throws SQLException {
        QueryStub stub = queryStub(List.of(
                Map.of("wide_id", 1L, "name", "A")
        ));
        DuckDbOperator operator = stub.operator;

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                List.of("wide_id"),
                "wide_users",
                "users",
                List.of(),
                Map.of(),
                operator,
                Map.of("users", schema("node1", "users")),
                "SELECT id AS wide_id, name FROM users"
        );

        SmartMerger.MergedRecord r = new SmartMerger.MergedRecord();
        r.getMainTableAfterPks().add(Map.of("id", 1L));
        r.setAfterRow("id=1", Map.of("id", 1L, "name", "A"));

        AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(List.of(r), "users");
        assertFalse(result.isEmpty());
        assertEquals(List.of(Map.of("wide_id", 1L)), result.getWideTablePks());
        assertEquals(1, result.getWideTableQueryResults().size());
        assertEquals(1, result.getAfterRows().size());
    }

    @Test
    void calculateAffectedAfterKeys_subTable_usesCteAndReturnsPks() throws SQLException {
        QueryStub stub = queryStub(List.of(
                Map.of("id", 1L, "v", "x"),
                Map.of("id", 2L, "v", "y")
        ));
        DuckDbOperator operator = stub.operator;

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
        assertEquals(1, stub.queries.get());
        assertEquals(2, result.getWideTablePks().size());
        assertEquals(2, result.getWideTableQueryResults().size());
    }

    @SafeVarargs
    private static QueryStub queryStub(List<Map<String, Object>>... responses) {
        AtomicInteger queries = new AtomicInteger();
        DuckDbOperator operator = (DuckDbOperator) Proxy.newProxyInstance(
                DuckDbOperator.class.getClassLoader(),
                new Class<?>[]{DuckDbOperator.class},
                (proxy, method, args) -> {
                    if ("executeQuery".equals(method.getName())) {
                        int index = queries.getAndIncrement();
                        if (responses.length == 0) {
                            return List.of();
                        }
                        return responses[Math.min(index, responses.length - 1)];
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if ("toString".equals(method.getName())) {
                        return "QueryStubDuckDbOperator";
                    }
                    throw new UnsupportedOperationException(method.getName());
                }
        );
        return new QueryStub(operator, queries);
    }

    private static class QueryStub {
        private final DuckDbOperator operator;
        private final AtomicInteger queries;

        private QueryStub(DuckDbOperator operator, AtomicInteger queries) {
            this.operator = operator;
            this.queries = queries;
        }
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
