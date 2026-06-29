package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.mongo.ClientMongoOperator;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqWriterTest {
    @Test
    void write_requiresMongoTemplate() {
        DlqWriter writer = new DlqWriter(null, "c");
        assertThrows(IllegalStateException.class, () -> writer.write("k", "t", List.of(), new RuntimeException("x")));

        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        when(clientMongoOperator.getMongoTemplate()).thenReturn(null);
        DlqWriter writer2 = new DlqWriter(clientMongoOperator, "c");
        assertThrows(IllegalStateException.class, () -> writer2.write("k", "t", List.of(), new RuntimeException("x")));
    }

    @Test
    void write_insertsDocumentWithExpectedFields() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        when(clientMongoOperator.getMongoTemplate()).thenReturn(mongoTemplate);

        DlqWriter writer = new DlqWriter(clientMongoOperator, "duckdb_dlq");

        SmartMerger.MergedRecord mergedRecord = new SmartMerger.MergedRecord();
        mergedRecord.setTableName("users");
        mergedRecord.getBeforeRows().add(Map.of("id", 1L));
        mergedRecord.setAfterRow("id=1", Map.of("id", 1L, "name", "A"));
        mergedRecord.getMainTableBeforePks().add(Map.of("id", 1L));
        mergedRecord.getMainTableAfterPks().add(Map.of("id", 1L));

        SQLException error = new SQLException("bad", "state", 123);
        writer.write(
                "ctx",
                "target",
                List.of(Map.of("id", 1L)),
                error,
                "task1",
                "batch1",
                "FAILED SQL",
                mergedRecord
        );

        ArgumentCaptor<Document> docCaptor = ArgumentCaptor.forClass(Document.class);
        verify(mongoTemplate).insert(docCaptor.capture(), org.mockito.ArgumentMatchers.eq("duckdb_dlq"));

        Document doc = docCaptor.getValue();
        assertEquals("ctx", doc.getString("contextKey"));
        assertEquals("target", doc.getString("targetTableName"));
        assertEquals("task1", doc.getString("taskId"));
        assertEquals("batch1", doc.getString("syncBatchId"));
        assertEquals("FAILED SQL", doc.getString("failedSql"));
        assertEquals("bad", doc.getString("errorMessage"));
        assertTrue(doc.containsKey("createdAt"));
        assertTrue(doc.containsKey("dlqTimestamp"));

        Document errorDoc = (Document) doc.get("error");
        assertNotNull(errorDoc);
        assertEquals("SQLException", errorDoc.getString("type"));
        assertEquals("bad", errorDoc.getString("message"));
        assertEquals(123, errorDoc.getInteger("code"));

        Document mergedDoc = (Document) doc.get("mergedRecordState");
        assertNotNull(mergedDoc);
        assertEquals("users", mergedDoc.getString("tableName"));
        assertTrue(mergedDoc.containsKey("beforeRows"));
        assertTrue(mergedDoc.containsKey("afterRows"));
    }

    @Test
    void writeForEvent_delegatesToWrite() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        when(clientMongoOperator.getMongoTemplate()).thenReturn(mongoTemplate);
        DlqWriter writer = new DlqWriter(clientMongoOperator, "duckdb_dlq");

        writer.writeForEvent("ctx", "target", Map.of("id", 1L), new RuntimeException("x"), "task", "batch", "sql");

        verify(mongoTemplate).insert(org.mockito.ArgumentMatchers.any(Document.class), org.mockito.ArgumentMatchers.eq("duckdb_dlq"));
    }
}

