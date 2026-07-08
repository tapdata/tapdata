package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WideTableIncrementalUpdaterTest {
    @Test
    void updateWideTableAsTapDataEvents_usesPrecomputedResults_andCallsConsumer() throws SQLException, IOException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);

        NodeSchemaInfo schemaInfo = schema("node1", "wide", List.of("id"), List.of("id", "v"));

        WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
                "wide",
                List.of("id"),
                "SELECT id, v FROM wide",
                generator,
                operator,
                false,
                schemaInfo
        ).log(logger);

        List<Map<String, Object>> beforeKeys = List.of(Map.of("id", 1L));
        List<Map<String, Object>> queryResults = List.of(Map.of("id", 1L, "v", "new"));
        List<Map<String, Object>> afterRows = List.of(Map.of("id", 1L, "v", "new"));

        List<TapdataEvent> consumed = new ArrayList<>();
        AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> consumerRef =
                new AtomicReference<>((event, result) -> consumed.add(event));

        List<TapdataEvent> events = updater.updateWideTableAsTapDataEvents(beforeKeys, queryResults, afterRows, "wide", consumerRef);

        assertFalse(events.isEmpty());
        assertEquals(events.size(), consumed.size());
        verify(operator, never()).executeUpdate(anyString());
    }

    @Test
    void updateWideTableAsTapDataEvents_withoutAfterRows_skipsQueryAndStillEmitsDeleteEvent() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);
        NodeSchemaInfo schemaInfo = schema("node1", "wide", List.of("id"), List.of("id", "v"));

        WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
                "wide",
                List.of("id"),
                "SELECT id, v FROM wide",
                generator,
                operator,
                false,
                schemaInfo
        ).log(logger);

        List<TapdataEvent> consumed = new ArrayList<>();
        AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> consumerRef =
                new AtomicReference<>((event, result) -> consumed.add(event));

        List<TapdataEvent> events = updater.updateWideTableAsTapDataEvents(
                List.of(Map.of("id", 1L)),
                List.of(),
                List.of(),
                "wide",
                consumerRef
        );

        assertEquals(events.size(), consumed.size());
        assertFalse(events.isEmpty());
        verify(generator, never()).generateBatch(anyString(), anyString(), any(), any());
        verify(operator, never()).executeQuery(anyString());
    }

    @Test
    void updateWideTableAsTapDataEvents_innerJoinChildDeleteEmitsDeleteWithoutRetain() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);
        List<String> widePk = List.of("warehouse_id", "district_id", "order_id", "item_id", "line_number");
        NodeSchemaInfo wideSchema = schema("wide", "wide_orders", widePk,
                List.of("warehouse_id", "district_id", "order_id", "item_id", "line_number", "quantity"));

        WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
                "wide_orders",
                widePk,
                "SELECT o.o_w_id AS warehouse_id, o.o_d_id AS district_id, o.o_id AS order_id, "
                        + "l.ol_i_id AS item_id, l.ol_number AS line_number, l.ol_quantity AS quantity "
                        + "FROM orders o INNER JOIN order_line l ON o.o_id=l.ol_o_id",
                generator,
                operator,
                false,
                wideSchema
        ).log(logger).withDeleteSemantics(WideTableSourceRegistry.from(
                "orders",
                List.of(
                        new FromTableConfig("pre_orders", "orders"),
                        new FromTableConfig("pre_order_line", "order_line")
                ),
                Map.of(
                        "pre_orders", schema("pre_orders", "orders", List.of("o_id"), List.of("o_id", "o_w_id", "o_d_id")),
                        "pre_order_line", schema("pre_order_line", "order_line", List.of("ol_o_id", "ol_number"),
                                List.of("ol_o_id", "ol_number", "ol_i_id", "ol_quantity"))
                ),
                "SELECT o.o_w_id AS warehouse_id, o.o_d_id AS district_id, o.o_id AS order_id, "
                        + "l.ol_i_id AS item_id, l.ol_number AS line_number, l.ol_quantity AS quantity "
                        + "FROM orders o INNER JOIN order_line l ON o.o_id=l.ol_o_id"
        ));
        when(operator.executeQuery(anyString())).thenReturn(List.of(Map.of(
                "warehouse_id", 1,
                "district_id", 1,
                "order_id", 2,
                "item_id", 1,
                "line_number", 1,
                "quantity", 2
        )));

        List<TapdataEvent> consumed = new ArrayList<>();
        AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> consumerRef =
                new AtomicReference<>((event, result) -> consumed.add(event));
        List<Map<String, Object>> beforeKeys = List.of(Map.of(
                "warehouse_id", 1,
                "district_id", 1,
                "order_id", 2,
                "item_id", 1,
                "line_number", 1
        ));

        List<TapdataEvent> events = updater.updateWideTableAsTapDataEvents(
                beforeKeys,
                List.of(),
                List.of(),
                "order_line",
                consumerRef
        );

        assertEquals(1, events.size());
        assertEquals(events, consumed);
        assertInstanceOf(TapDeleteRecordEvent.class, events.get(0).getTapEvent());
        verify(operator, never()).executeQuery(anyString());
    }

    @Test
    void privateHelpers_coverFallbackAndConversionBranches() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        WideTableIncrementalUpdater updaterWithoutSchema = new WideTableIncrementalUpdater(
                "wide",
                List.of("id"),
                "SELECT id FROM source_table",
                generator,
                operator,
                true,
                null
        ).log(mock(ObsLogger.class));

        when(operator.executeQuery(anyString())).thenReturn(List.of(Map.of("cnt", 1)));

        invokePrivate(
                updaterWithoutSchema,
                "applyWideTableChanges",
                new Class[]{List.class, List.class},
                new Object[]{List.of(Map.of("id", 1)), List.of(Map.of("id", 2, "v", "x"))}
        );

        verify(operator).executeUpdate(anyString());
        verify(operator).writeBatch(List.of(Map.of("id", 2, "v", "x")), "wide");

        assertEquals(123L, invokePrivate(
                updaterWithoutSchema,
                "convertPkValue",
                new Class[]{Object.class, Class.class},
                new Object[]{"123", Long.class}
        ));
        assertEquals("bad", invokePrivate(
                updaterWithoutSchema,
                "convertPkValue",
                new Class[]{Object.class, Class.class},
                new Object[]{"bad", Long.class}
        ));
        assertEquals("99", invokePrivate(
                updaterWithoutSchema,
                "convertPkValue",
                new Class[]{Object.class, Class.class},
                new Object[]{99, String.class}
        ));
        assertNull(invokePrivate(
                updaterWithoutSchema,
                "convertPkValue",
                new Class[]{Object.class, Class.class},
                new Object[]{null, String.class}
        ));

        @SuppressWarnings("unchecked")
        Map<String, Class<?>> defaultPkTypes = (Map<String, Class<?>>) invokePrivate(
                updaterWithoutSchema,
                "getPkTargetType",
                new Class[]{},
                new Object[]{}
        );
        assertEquals(String.class, defaultPkTypes.get("id"));

        TapTable typedTapTable = new TapTable();
        LinkedHashMap<String, TapField> typedFields = new LinkedHashMap<>();
        TapField idField = new TapField();
        idField.setName("id");
        idField.setOriginalFieldName("id");
        idField.setDataType("BIGINT");
        idField.setTapType(new TapNumber());
        typedFields.put("id", idField);
        TapField flagField = new TapField();
        flagField.setName("flag");
        flagField.setOriginalFieldName("flag");
        flagField.setDataType("BOOLEAN");
        flagField.setTapType(new TapBoolean());
        typedFields.put("flag", flagField);
        typedTapTable.setNameFieldMap(typedFields);
        NodeSchemaInfo typedSchema = new NodeSchemaInfo(
                "node2",
                "wide2",
                "wide2",
                List.of("id", "flag"),
                typedFields,
                typedTapTable,
                null
        );
        WideTableIncrementalUpdater updaterWithSchema = new WideTableIncrementalUpdater(
                "wide2",
                List.of("id", "flag"),
                "SELECT id, flag FROM wide2",
                generator,
                operator,
                false,
                typedSchema
        );

        @SuppressWarnings("unchecked")
        Map<String, Class<?>> typedPkTypes = (Map<String, Class<?>>) invokePrivate(
                updaterWithSchema,
                "getPkTargetType",
                new Class[]{},
                new Object[]{}
        );
        assertEquals(Long.class, typedPkTypes.get("id"));
        assertEquals(Boolean.class, typedPkTypes.get("flag"));
    }

    @Test
    void ensureWideTableExists_fallsBackToNodeSchemaDdl_whenAsSelectFails() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);
        NodeSchemaInfo schemaInfo = schema("node1", "wide", List.of("id"), List.of("id", "v"));

        when(operator.executeQuery(anyString())).thenReturn(List.of(Map.of("cnt", 0)));
        when(operator.executeUpdate(anyString()))
                .thenThrow(new SQLException("as select failed"))
                .thenReturn(0)
                .thenReturn(0);

        WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
                "wide",
                List.of("id"),
                "SELECT id, v FROM source_table",
                generator,
                operator,
                false,
                schemaInfo
        ).log(logger);

        invokePrivate(updater, "ensureWideTableExists", new Class[]{}, new Object[]{});

        verify(operator, times(3)).executeUpdate(anyString());
    }

    @Test
    void changelogListener_methods_coverNullAndExceptionPaths() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);
        WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
                "wide",
                List.of("id"),
                "SELECT id FROM wide",
                generator,
                operator,
                false,
                schema("node1", "wide", List.of("id"), List.of("id"))
        ).log(logger);

        updater.addChangelogListener(null);
        updater.addChangelogListener(event -> {
            throw new IllegalStateException("listener failed");
        });
        updater.addChangelogListener(event -> {
        });

        invokePrivate(
                updater,
                "emitWideTableChangelogEvents",
                new Class[]{List.class},
                new Object[]{List.of(new TapdataEvent())}
        );

        verify(logger).warn(eq("Error notifying changelog listener"), any(Exception.class));
    }

    private static NodeSchemaInfo schema(String nodeId, String tableName, List<String> pks, List<String> fieldNames) {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        for (String fieldName : fieldNames) {
            TapField field = new TapField();
            field.setName(fieldName);
            field.setOriginalFieldName(fieldName);
            if ("id".equals(fieldName)) {
                field.setDataType("BIGINT");
                field.setTapType(new TapNumber());
            } else {
                field.setDataType("VARCHAR");
            }
            fields.put(fieldName, field);
        }
        tapTable.setNameFieldMap(fields);
        return new NodeSchemaInfo(nodeId, tableName, tableName, pks, fields, tapTable, null);
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object[] args) throws Exception {
        java.lang.reflect.Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
