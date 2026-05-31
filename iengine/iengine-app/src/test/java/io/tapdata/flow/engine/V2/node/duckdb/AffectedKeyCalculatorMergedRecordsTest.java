package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class AffectedKeyCalculatorMergedRecordsTest {
    @Test
    void calculateAffectedBeforeKeys_usesMergedRecordsAndIncludesDeleteBefore() throws SQLException {
        CapturingWithCteSqlGenerator generator = new CapturingWithCteSqlGenerator();
        DuckDbOperator operator = Mockito.mock(DuckDbOperator.class);
        Mockito.when(operator.executeQuery(Mockito.anyString()))
                .thenReturn(List.of(Collections.singletonMap("wide_id", 1)));

        List<FromTableConfig> fromTables = List.of(
                new FromTableConfig("source", "id"),
                        "SELECT t.id AS wide_id FROM source t", List.of("id", "name"))
        );

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "wide_id", "main", "id", fromTables, Collections.emptyMap(), operator, generator
        );

        TapInsertRecordEvent insert = TapInsertRecordEvent.create()
                .table("source")
                .after(Map.of("id", 1, "name", "insert_after"));
        TapUpdateRecordEvent update = TapUpdateRecordEvent.create()
                .table("source")
                .before(Map.of("id", 1, "name", "update_before"))
                .after(Map.of("id", 1, "name", "b"));
        TapDeleteRecordEvent delete = TapDeleteRecordEvent.create()
                .table("source")
                .before(Map.of("id", 1, "name", "delete_before"));

        SmartMerger.MergedRecord record = new SmartMerger.MergedRecord(1, 1);
        record.getOperations().add(wrap(insert));
        record.getOperations().add(wrap(update));
        record.getOperations().add(wrap(delete));
        record.setFinalStateEvent(update);
        record.setFinalOp("DELETE");

        Set<Object> keys = calculator.calculateAffectedBeforeKeys(List.of(record), "source");

        assertEquals(List.of("id", "name"), generator.lastFields);
        assertEquals(Set.of(1), keys);
        assertNotNull(generator.lastRows);
        assertEquals(3, generator.lastRows.size());
        List<String> names = new ArrayList<>();
        for (Map<String, Object> row : generator.lastRows) names.add((String) row.get("name"));
        assertTrue(names.contains("insert_after"));
        assertTrue(names.contains("update_before"));
        assertTrue(names.contains("delete_before"));
    }

    @Test
    void calculateAffectedAfterKeys_groupsByTableFromMergedRecords() throws SQLException {
        CapturingWithCteSqlGenerator generator = new CapturingWithCteSqlGenerator();
        DuckDbOperator operator = Mockito.mock(DuckDbOperator.class);
        Mockito.when(operator.executeQuery(Mockito.anyString()))
                .thenReturn(List.of(Collections.singletonMap("wide_id", 1)));

        List<FromTableConfig> fromTables = List.of(
                new FromTableConfig("source_a", "id"),
                        "SELECT t.id AS wide_id FROM source_a t", List.of("id")),
                new FromTableConfig("source_b", "id"),
                        "SELECT t.id AS wide_id FROM source_b t", List.of("id"))
        );

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "wide_id", "main", "id", fromTables, Collections.emptyMap(), operator, generator
        );

        SmartMerger.MergedRecord recordA = new SmartMerger.MergedRecord(1, 1);
        TapInsertRecordEvent insertA = TapInsertRecordEvent.create()
                .table("source_a")
                .after(Map.of("id", 1));
        recordA.getOperations().add(wrap(insertA));
        recordA.setFinalStateEvent(insertA);

        SmartMerger.MergedRecord recordB = new SmartMerger.MergedRecord(2, 2);
        TapInsertRecordEvent insertB = TapInsertRecordEvent.create()
                .table("source_b")
                .after(Map.of("id", 2));
        recordB.getOperations().add(wrap(insertB));
        recordB.setFinalStateEvent(insertB);

        calculator.calculateAffectedAfterKeys(List.of(recordA, recordB));

        assertEquals(Set.of("source_a", "source_b"),
                new HashSet<>(generator.calledTableNames));
    }

    private TapdataEvent wrap(TapRecordEvent event) {
        TapdataEvent dataEvent = new TapdataEvent();
        dataEvent.setTapEvent(event);
        return dataEvent;
    }

    private static class CapturingWithCteSqlGenerator extends WithCteSqlGenerator {
        List<Map<String, Object>> lastRows;
        List<String> lastFields;
        List<String> calledTableNames = new ArrayList<>();

        @Override
        public String generateBatch(String sqlTemplate, String tableName,
                                    List<Map<String, Object>> rows, List<String> fields) {
            this.lastRows = new ArrayList<>(rows);
            this.lastFields = new ArrayList<>(fields);
            this.calledTableNames.add(tableName);
            return "SELECT 1 AS wide_id";
        }
    }
}
