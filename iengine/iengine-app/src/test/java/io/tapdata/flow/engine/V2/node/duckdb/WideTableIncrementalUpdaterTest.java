package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    void incrementalViewUpdater_updateWideTable_executesDiffsAndEmitsChangelog() throws Exception {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        doAnswer(invocation -> {
            DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
            action.accept();
            return null;
        }).when(operator).executeInTransaction(any());

        when(operator.queryForMap(anyString(), anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.contains("FROM wide_table")) {
                return Map.of(
                        1L, Map.of("id", 1L, "v", "old")
                );
            }
            return Map.of(
                    1L, Map.of("id", 1L, "v", "new"),
                    2L, Map.of("id", 2L, "v", "ins")
            );
        });

        IncrementalViewUpdater updater = new IncrementalViewUpdater(
                "wide_table",
                "id",
                "SELECT id, v FROM source",
                true,
                operator
        );

        List<Map<String, Object>> changelog = new ArrayList<>();
        updater.addChangelogListener(changelog::add);

        int updated = updater.updateWideTable(Set.of(1L, 2L));
        assertEquals(2, updated);
        assertFalse(changelog.isEmpty());
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
}
