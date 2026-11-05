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
public class Query {

    private final MongoOperatorFactory mongoOperatorFactory;

    public Query(MongoOperatorFactory mongoOperatorFactory) {
        this.mongoOperatorFactory = mongoOperatorFactory;
    }

    @McpTool(name = "query", description = "Execute a MongoDB find query through a TapData MongoDB connection.")
    public List<Document> query(
            McpSyncRequestContext context,
            @McpToolParam(description = "TapData MongoDB connection id.") String connectionId,
            @McpToolParam(description = "MongoDB collection name.") String collectionName,
            @McpToolParam(required = false, description = "MongoDB query filter document.") Map<String, Object> filter,
            @McpToolParam(required = false, description = "MongoDB projection document.") Map<String, Object> projection,
            @McpToolParam(required = false, description = "Maximum number of documents to return.") Integer limit,
            @McpToolParam(required = false, description = "Number of documents to skip.") Integer skip,
            @McpToolParam(required = false, description = "Maximum execution time in milliseconds.") Long maxTimeMS,
            @McpToolParam(required = false, description = "Explain verbosity, such as queryPlanner, executionStats, or allPlansExecution.") String explain) {
        if (StringUtils.isBlank(collectionName)) {
            throw new RuntimeException("Parameter collectionName is required");
        }

        Map<String, Object> params = new java.util.LinkedHashMap<>();
        params.put("filter", filter);
        params.put("projection", projection);
        params.put("limit", limit);
        params.put("skip", skip);
        params.put("maxTimeMS", maxTimeMS);
        params.put("explain", explain);

        try (MongoOperator mongoOperator = mongoOperatorFactory.create(context, connectionId)){

            mongoOperator.connect();
            return mongoOperator.query(collectionName, params);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
