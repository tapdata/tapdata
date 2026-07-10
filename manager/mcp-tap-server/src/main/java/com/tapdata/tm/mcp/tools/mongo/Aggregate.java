package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.mcp.mongodb.MongoOperator;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 19:24
 */
@Component
public class Aggregate {

    private final MongoOperatorFactory mongoOperatorFactory;

    public Aggregate(MongoOperatorFactory mongoOperatorFactory) {
        this.mongoOperatorFactory = mongoOperatorFactory;
    }

    @McpTool(name = "aggregate", description = "Execute a MongoDB aggregation pipeline through a TapData MongoDB connection. In aggregation expressions, do not use Extended JSON wrappers like {$date: '$field'}; use {$toDate: '$field'} for field conversion and ISO-8601 strings only for date literals.")
    public List<Document> aggregate(
            McpSyncRequestContext context,
            @McpToolParam(description = "TapData MongoDB connection id.") String connectionId,
            @McpToolParam(description = "MongoDB collection name.") String collectionName,
            @McpToolParam(description = "MongoDB aggregation pipeline stages. Use normal MongoDB aggregation operators, for example {$toDate: '$created_at'} instead of {$date: '$created_at'}.") List<Map<String, Object>> pipeline,
            @McpToolParam(required = false, description = "Explain verbosity, such as queryPlanner, executionStats, or allPlansExecution.") String explain) {
        if (StringUtils.isBlank(collectionName)) {
            throw new RuntimeException("Parameter collectionName is required");
        }

        Map<String, Object> params = new java.util.LinkedHashMap<>();
        params.put("pipeline", pipeline);
        params.put("explain", explain);

        try (MongoOperator mongoOperator = mongoOperatorFactory.create(context, connectionId)){

            mongoOperator.connect();
            return mongoOperator.aggregate(collectionName, params);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
