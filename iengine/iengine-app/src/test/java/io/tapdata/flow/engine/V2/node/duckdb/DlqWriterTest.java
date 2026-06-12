package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.mongo.ClientMongoOperator;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqWriterTest {

    @Test
    void writesDlqDocumentToMongoCollection() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        when(clientMongoOperator.getMongoTemplate()).thenReturn(mongoTemplate);

        DlqWriter writer = new DlqWriter(clientMongoOperator, "duckdb_dlq_records");
        List<Map<String, Object>> payload = List.of(Map.of("id", 1, "name", "row-1"));

        assertDoesNotThrow(() -> writer.write("source-a:orders", "source_a__orders", payload, new IllegalStateException("boom")));

        verify(mongoTemplate).insert(any(Document.class), eq("duckdb_dlq_records"));
    }
}
