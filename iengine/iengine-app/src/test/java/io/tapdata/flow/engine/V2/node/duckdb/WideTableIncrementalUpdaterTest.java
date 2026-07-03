package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
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
    void updateWideTableAsTapDataEvents_whenNoPrecomputedResults_executesWithCteQuery() throws SQLException, IOException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);

        NodeSchemaInfo schemaInfo = schema("node1", "wide", List.of("id"), List.of("id", "v"));

        when(generator.generateBatch(anyString(), anyString(), any(), any())).thenReturn("WITH ...");
        when(operator.executeQuery(anyString())).thenReturn(List.of(Map.of("id", 1L, "v", "x")));

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
        List<Map<String, Object>> afterRows = List.of(Map.of("id", 1L, "v", "x"));

        AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> consumerRef =
                new AtomicReference<>((event, result) -> {});

        List<TapdataEvent> events = updater.updateWideTableAsTapDataEvents(beforeKeys, List.of(), afterRows, "wide", consumerRef);
        assertFalse(events.isEmpty());
        verify(generator).generateBatch(anyString(), anyString(), any(), any());
        verify(operator).executeQuery(anyString());
    }

    @Test
    void updateWideTableAsTapDataEvents_whenWriteEnabled_usesSchemaAwareDeleteAndInsert() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        WithCteSqlGenerator generator = mock(WithCteSqlGenerator.class);
        ObsLogger logger = mock(ObsLogger.class);
        NodeSchemaInfo schemaInfo = schema("node1", "wide", List.of("id"), List.of("id", "flag"));

        when(operator.executeQuery(anyString())).thenReturn(List.of(Map.of("cnt", 1)));

        WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
                "wide",
                List.of("id"),
                "SELECT id, flag FROM wide",
                generator,
                operator,
                true,
                schemaInfo
        ).log(logger);

        AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> consumerRef =
                new AtomicReference<>((event, result) -> {});

        List<Map<String, Object>> beforeKeys = List.of(Map.of("id", "1"));
        List<Map<String, Object>> queryResults = List.of(Map.of("id", 1L, "flag", true));
        List<Map<String, Object>> afterRows = List.of(Map.of("id", 1L, "flag", true));

        List<TapdataEvent> events = updater.updateWideTableAsTapDataEvents(beforeKeys, queryResults, afterRows, "wide", consumerRef);

        assertFalse(events.isEmpty());
        verify(operator).deleteByIds(any(), eq(schemaInfo));
        verify(operator).writeBatch(queryResults, schemaInfo);
        verify(operator, never()).executeUpdate("DROP TABLE IF EXISTS wide");
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
