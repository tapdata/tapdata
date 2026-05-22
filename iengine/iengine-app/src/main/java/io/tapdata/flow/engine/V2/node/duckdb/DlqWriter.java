package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.mongo.ClientMongoOperator;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Writes failed DuckDB flush payloads to MongoDB DLQ collection.
 */
public class DlqWriter {

    private final ClientMongoOperator clientMongoOperator;
    private final String collectionName;

    public DlqWriter(ClientMongoOperator clientMongoOperator, String collectionName) {
        this.clientMongoOperator = clientMongoOperator;
        this.collectionName = collectionName;
    }

    public void write(String contextKey, String targetTableName, List<Map<String, Object>> payload, Exception error) {
        if (clientMongoOperator == null || clientMongoOperator.getMongoTemplate() == null) {
            throw new IllegalStateException("MongoTemplate is required for DLQ writes");
        }

        MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
        Document document = new Document()
                .append("contextKey", contextKey)
                .append("targetTableName", targetTableName)
                .append("payload", payload)
                .append("errorMessage", error == null ? null : error.getMessage())
                .append("errorClass", error == null ? null : error.getClass().getName())
                .append("createdAt", Instant.now().toString());

        mongoTemplate.insert(document, collectionName);
    }
}
