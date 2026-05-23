package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.mongo.ClientMongoOperator;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes failed DuckDB flush payloads to MongoDB DLQ collection.
 * Fully conforms to D6 decision (ADR decision-5-7-complete.md).
 */
public class DlqWriter {

    private final ClientMongoOperator clientMongoOperator;
    private final String collectionName;

    public DlqWriter(ClientMongoOperator clientMongoOperator, String collectionName) {
        this.clientMongoOperator = clientMongoOperator;
        this.collectionName = collectionName;
    }

    /**
     * Full D6-compliant write with all metadata.
     */
    public void write(String contextKey,
                      String targetTableName,
                      List<Map<String, Object>> payload,
                      Exception error,
                      String taskId,
                      String syncBatchId,
                      String failedSql,
                      SmartMerger.MergedRecord mergedRecordState) {
        if (clientMongoOperator == null || clientMongoOperator.getMongoTemplate() == null) {
            throw new IllegalStateException("MongoTemplate is required for DLQ writes");
        }

        MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

        Document errorDoc = null;
        if (error != null) {
            errorDoc = new Document()
                    .append("type", error.getClass().getSimpleName())
                    .append("message", error.getMessage())
                    .append("code", extractErrorCode(error));
        }

        Document mergedRecordDoc = null;
        if (mergedRecordState != null) {
            mergedRecordDoc = new Document()
                    .append("initialPk", mergedRecordState.getInitialPk())
                    .append("currentPk", mergedRecordState.getCurrentPk())
                    .append("operations", mergedRecordState.getOperations())
                    .append("finalState", mergedRecordState.getFinalState())
                    .append("finalOp", mergedRecordState.getFinalOp());
        }

        Document document = new Document()
                .append("contextKey", contextKey)
                .append("targetTableName", targetTableName)
                .append("payload", payload)
                .append("taskId", taskId)
                .append("syncBatchId", syncBatchId)
                .append("dlqTimestamp", Instant.now().toString())
                .append("failedSql", failedSql)
                .append("error", errorDoc)
                .append("errorMessage", error == null ? null : error.getMessage())
                .append("errorClass", error == null ? null : error.getClass().getName())
                .append("createdAt", Instant.now().toString())
                .append("retryCount", 0)
                .append("lastRetryAt", null)
                .append("manualResolution", null)
                .append("mergedRecordState", mergedRecordDoc);

        mongoTemplate.insert(document, collectionName);
    }

    /**
     * Backward-compatible write (for existing code).
     */
    @Deprecated
    public void write(String contextKey, String targetTableName, List<Map<String, Object>> payload, Exception error) {
        write(contextKey, targetTableName, payload, error, null, null, null, null);
    }

    /**
     * Extract error code if available (from SQLException or similar).
     */
    private Integer extractErrorCode(Exception error) {
        try {
            // Check for SQL-specific error codes
            if (Class.forName("java.sql.SQLException").isInstance(error)) {
                java.lang.reflect.Method method = error.getClass().getMethod("getErrorCode");
                Object code = method.invoke(error);
                if (code instanceof Integer) {
                    return (Integer) code;
                }
            }
        } catch (Exception ignored) {
            // Fall through
        }
        return null;
    }

    /**
     * Create a simple DLQ record for an event without merged state.
     */
    public void writeForEvent(String contextKey,
                              String targetTableName,
                              Map<String, Object> event,
                              Exception error,
                              String taskId,
                              String syncBatchId,
                              String failedSql) {
        write(contextKey, targetTableName, List.of(event), error, taskId, syncBatchId, failedSql, null);
    }
}
