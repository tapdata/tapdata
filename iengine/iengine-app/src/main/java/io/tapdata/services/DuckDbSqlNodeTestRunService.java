package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.sql.SQLException;
import java.util.*;

/**
 * DuckDB SQL 节点测试运行服务 - Engine 端
 * 统一使用 DuckDbOperator 工具类处理 DuckDB 操作
 */
@RemoteService
@Slf4j
public class DuckDbSqlNodeTestRunService {

    private static final String DLQ_COLLECTION = "duckdb_dlq_records";
    private static final int DEFAULT_MAX_ROWS = 100;
    private static final int MAX_LIMIT = 1000;
    private static final int MIN_LIMIT = 1;

    public DuckDbSqlTestResponse testSql(DuckDbSqlTestRequest request) {
        DuckDbSqlTestResponse response = new DuckDbSqlTestResponse();
        long startTs = System.currentTimeMillis();

        int maxRows = request.getRows() != null ? request.getRows() : DEFAULT_MAX_ROWS;
        String sql = request.getSql();

        try (DuckDbOperator operator = new DuckDbOperatorImpl()) {
            DuckDbOperator.ExecuteResult result = operator.execute(sql);
            
            if (result.isHasResultSet()) {
                List<Map<String, Object>> rows = result.getResultSet();
                // 限制返回行数，创建新的 ArrayList 避免视图问题
                if (rows.size() > maxRows) {
                    rows = new ArrayList<>(rows.subList(0, maxRows));
                }
                response.setRows(rows);
                response.setRowCount(rows.size());
            } else {
                response.setRowCount(result.getUpdateCount());
            }
            response.setSuccess(true);
        } catch (SQLException e) {
            log.error("Error executing SQL test: {}", e.getMessage(), e);
            response.setSuccess(false);
            response.setErrorMessage(e.getMessage());
            response.setErrorType(e.getClass().getSimpleName());
        } finally {
            response.setExecutionTimeMs(System.currentTimeMillis() - startTs);
        }

        return response;
    }

    public DuckDbTableSchemaDto getTableSchema(String tableName) {
        DuckDbTableSchemaDto dto = new DuckDbTableSchemaDto();
        dto.setTableName(tableName);
        Map<String, String> fields = new HashMap<>();

        try (DuckDbOperator operator = new DuckDbOperatorImpl()) {
            List<Map<String, Object>> columns = operator.getTableColumns(tableName);

            for (Map<String, Object> columnInfo : columns) {
                String columnName = Objects.toString(columnInfo.get("column_name"), null);
                String dataType = Objects.toString(columnInfo.get("data_type"), null);
                String isNullable = Objects.toString(columnInfo.get("is_nullable"), null);

                if (columnName != null && dataType != null) {
                    String typeDesc = dataType;
                    if ("YES".equals(isNullable)) {
                        typeDesc += " NULLABLE";
                    } else {
                        typeDesc += " NOT NULL";
                    }
                    fields.put(columnName, typeDesc);
                }
            }

            dto.setFields(fields);
            dto.setSuccess(true);
        } catch (SQLException e) {
            log.error("Error getting table schema for {}: {}", tableName, e.getMessage(), e);
            dto.setSuccess(false);
            dto.setErrorMessage(e.getMessage());
            dto.setErrorType(e.getClass().getSimpleName());
        }

        return dto;
    }

    public List<DuckDbDeadLetterDto> listDeadLetters(String nodeId, int limit) {
        List<DuckDbDeadLetterDto> result = new ArrayList<>();

        // 边界检查 limit
        int safeLimit = Math.max(MIN_LIMIT, Math.min(limit, MAX_LIMIT));

        try {
            ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
            if (clientMongoOperator == null || clientMongoOperator.getMongoTemplate() == null) {
                log.error("MongoTemplate is not available");
                return result;
            }

            MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

            Query query = new Query();
            
            if (nodeId != null && !nodeId.isEmpty()) {
                query.addCriteria(Criteria.where("contextKey").regex(".*" + nodeId + ".*"));
            }
            
            query.with(Sort.by(Sort.Direction.DESC, "dlqTimestamp"));
            query.limit(safeLimit);

            List<Document> documents = mongoTemplate.find(query, Document.class, DLQ_COLLECTION);

            for (Document doc : documents) {
                DuckDbDeadLetterDto dto = new DuckDbDeadLetterDto();
                dto.setId(doc.getObjectId("_id").toHexString());
                dto.setContextKey(doc.getString("contextKey"));
                dto.setTargetTableName(doc.getString("targetTableName"));
                dto.setTaskId(doc.getString("taskId"));
                dto.setSyncBatchId(doc.getString("syncBatchId"));
                dto.setDlqTimestamp(doc.getString("dlqTimestamp"));
                dto.setFailedSql(doc.getString("failedSql"));
                dto.setErrorMessage(doc.getString("errorMessage"));
                dto.setErrorClass(doc.getString("errorClass"));
                dto.setPayload(doc.get("payload", List.class));
                dto.setRetryCount(doc.getInteger("retryCount"));
                dto.setLastRetryAt(doc.getString("lastRetryAt"));
                dto.setManualResolution(doc.get("manualResolution"));
                dto.setCreatedAt(doc.getString("createdAt"));
                result.add(dto);
            }

            log.info("Found {} dead letters for node: {}", result.size(), nodeId);
        } catch (Exception e) {
            log.error("Error listing dead letters: {}", e.getMessage(), e);
        }

        return result;
    }
}
