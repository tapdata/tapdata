package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.mcp.mongodb.MongoOperator;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 19:24
 */
@Component
public class Count {

    private final MongoOperatorFactory mongoOperatorFactory;

    public Count(MongoOperatorFactory mongoOperatorFactory) {
        this.mongoOperatorFactory = mongoOperatorFactory;
    }

    @McpTool(name = "count", description = "Count MongoDB documents that match a query through a TapData MongoDB connection.")
    public Map<String, Object> count(
            McpSyncRequestContext context,
            @McpToolParam(description = "TapData MongoDB connection id.") String connectionId,
            @McpToolParam(description = "MongoDB collection name.") String collectionName,
            @McpToolParam(required = false, description = "MongoDB count query filter document.") Map<String, Object> query,
            @McpToolParam(required = false, description = "Maximum number of documents to count.") Integer limit,
            @McpToolParam(required = false, description = "Number of documents to skip before counting.") Integer skip,
            @McpToolParam(required = false, description = "MongoDB hint document.") Map<String, Object> hint,
            @McpToolParam(required = false, description = "MongoDB read concern document.") Map<String, Object> readConcern,
            @McpToolParam(required = false, description = "Maximum execution time in milliseconds.") Long maxTimeMS,
            @McpToolParam(required = false, description = "MongoDB collation document.") Map<String, Object> collation) {
        if (StringUtils.isBlank(collectionName)) {
            throw new RuntimeException("Parameter collectionName is required");
        }

        Map<String, Object> params = new java.util.LinkedHashMap<>();
        params.put("query", query);
        params.put("limit", limit);
        params.put("skip", skip);
        params.put("hint", hint);
        params.put("readConcern", readConcern);
        params.put("maxTimeMS", maxTimeMS);
        params.put("collation", collation);

        try (MongoOperator mongoOperator = mongoOperatorFactory.create(context, connectionId)){

            mongoOperator.connect();
            long count = mongoOperator.count(collectionName, params);

            Map<String, Object> result = new HashMap<>();
            result.put("count", count);
            result.put("ok", 1);

            return result;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
