package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeleteAdjustmentTest {
    @Test
    void deleteAdjustmentService_selectsFirstSupportingStrategy() throws SQLException {
        WideTableDeleteAdjustmentStrategy s1 = new WideTableDeleteAdjustmentStrategy() {
            @Override
            public boolean supports(WideTableDeleteAdjustmentContext context) {
                return true;
            }

            @Override
            public List<Map<String, Object>> adjust(WideTableDeleteAdjustmentContext context) {
                return List.of(Map.of("id", 1));
            }
        };

        WideTableDeleteAdjustmentStrategy s2 = new WideTableDeleteAdjustmentStrategy() {
            @Override
            public boolean supports(WideTableDeleteAdjustmentContext context) {
                return true;
            }

            @Override
            public List<Map<String, Object>> adjust(WideTableDeleteAdjustmentContext context) {
                return List.of(Map.of("id", 2));
            }
        };

        WideTableDeleteAdjustmentService service = new WideTableDeleteAdjustmentService(List.of(s1, s2));
        List<Map<String, Object>> result = service.adjust(new WideTableDeleteAdjustmentContext().afterResults(List.of()));
        assertEquals(List.of(Map.of("id", 1)), result);
    }

    @Test
    void childTableDeleteRetainStrategy_supportsAndAdjustsByNullifyingOwnedFields() throws SQLException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        when(operator.executeQuery(anyString())).thenReturn(List.of(
                new LinkedHashMap<>(Map.of("id", 1L, "user_id", 1L, "order_amount", 10L, "other", "x"))
        ));

        NodeSchemaInfo childSchema = schema("node_orders", "orders", List.of("id"), List.of("order_amount"));
        NodeSchemaInfo mainSchema = schema("node_users", "users", List.of("id"), List.of("user_id"));

        WideTableSourceRegistry registry = WideTableSourceRegistry.from(
                "u",
                List.of(
                        new FromTableConfig("pre_users", "u"),
                        new FromTableConfig("pre_orders", "o")
                ),
                Map.of("pre_users", mainSchema, "pre_orders", childSchema)
        );

        WideTableFieldOwnershipResolver ownershipResolver = new WideTableFieldOwnershipResolver() {
            @Override
            public Set<String> resolveOwnedFields(String sourceTableName) {
                if ("orders".equalsIgnoreCase(sourceTableName)) {
                    return Set.of("order_amount");
                }
                return Set.of();
            }
        };

        SmartMerger.MergedRecord mergedRecord = new SmartMerger.MergedRecord();
        mergedRecord.getBeforeRows().add(Map.of("id", 1L));

        WideTableDeleteAdjustmentContext context = new WideTableDeleteAdjustmentContext()
                .sourceTableName("orders")
                .beforeKeys(List.of(Map.of("id", 1L)))
                .afterResults(List.of())
                .mergedRecords(List.of(mergedRecord))
                .wideTableName("wide_table")
                .wideTablePrimaryKey(List.of("id"))
                .duckDbOperator(operator)
                .sourceRegistry(registry)
                .fieldOwnershipResolver(ownershipResolver);

        ChildTableDeleteRetainStrategy strategy = new ChildTableDeleteRetainStrategy();
        assertTrue(strategy.supports(context));

        List<Map<String, Object>> adjusted = strategy.adjust(context);
        assertEquals(1, adjusted.size());
        assertEquals(1L, adjusted.get(0).get("id"));
        assertNull(adjusted.get(0).get("order_amount"));
        assertEquals("x", adjusted.get(0).get("other"));
    }

    @Test
    void childTableDeleteRetainStrategy_subWhere_buildsOrOfAndClauses() {
        ChildTableDeleteRetainStrategy strategy = new ChildTableDeleteRetainStrategy();
        WideTableDeleteAdjustmentContext context = new WideTableDeleteAdjustmentContext()
                .beforeKeys(List.of(
                        Map.of("id", 1, "tenant", "t1"),
                        Map.of("id", 2, "tenant", "t1")
                ));
        String where = strategy.subWhere(context);
        assertTrue(where.contains("id = 1"));
        assertTrue(where.contains("tenant = 't1'"));
        assertTrue(where.toUpperCase().contains("OR"));
        assertTrue(where.toUpperCase().contains("AND"));
    }

    @Test
    void jsqlParserWideTableFieldOwnershipResolver_resolvesOwnedFieldsByAlias() {
        NodeSchemaInfo users = schema("node_users", "users", List.of("id"), List.of("id", "name"));
        NodeSchemaInfo orders = schema("node_orders", "orders", List.of("id"), List.of("amount"));
        WideTableSourceRegistry registry = WideTableSourceRegistry.from(
                "u",
                List.of(
                        new FromTableConfig("pre_users", "u"),
                        new FromTableConfig("pre_orders", "o")
                ),
                Map.of("pre_users", users, "pre_orders", orders)
        );

        String sql = "SELECT u.id AS user_id, o.amount AS amount FROM users u LEFT JOIN orders o ON u.id=o.user_id";
        JSqlParserWideTableFieldOwnershipResolver resolver = new JSqlParserWideTableFieldOwnershipResolver(sql, registry);

        assertTrue(resolver.resolveOwnedFields("users").contains("user_id"));
        assertTrue(resolver.resolveOwnedFields("orders").contains("amount"));
    }

    @Test
    void wideTableFieldOwnershipResolver_noop_returnsEmpty() {
        WideTableFieldOwnershipResolver noop = WideTableFieldOwnershipResolver.noop();
        assertTrue(noop.resolveOwnedFields("any").isEmpty());
    }

    private static NodeSchemaInfo schema(String nodeId, String tableName, List<String> pks, List<String> fieldNames) {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        for (String fieldName : fieldNames) {
            TapField field = new TapField();
            field.setName(fieldName);
            field.setOriginalFieldName(fieldName);
            field.setDataType("id".equals(fieldName) ? "BIGINT" : "VARCHAR");
            if ("id".equals(fieldName)) {
                field.setTapType(new TapNumber());
            }
            fields.put(fieldName, field);
        }
        tapTable.setNameFieldMap(fields);
        return new NodeSchemaInfo(nodeId, tableName, tableName, pks, fields, tapTable, null);
    }
}

