package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.mcp.mongodb.MongoOperator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
@Component
public class ListCollection {

    private final MongoOperatorFactory mongoOperatorFactory;

    public ListCollection(MongoOperatorFactory mongoOperatorFactory) {
        this.mongoOperatorFactory = mongoOperatorFactory;
    }

    @McpTool(name = "listCollection", description = "List MongoDB collections through a TapData MongoDB connection.")
    public List<Object> listCollection(
            McpSyncRequestContext context,
            @McpToolParam(description = "TapData MongoDB connection id.") String connectionId,
            @McpToolParam(required = false, description = "Whether to return only collection names.") Boolean nameOnly) {
        boolean onlyNames = Boolean.TRUE.equals(nameOnly);

        try (MongoOperator mongoOperator = mongoOperatorFactory.create(context, connectionId)){

            mongoOperator.connect();

            return mongoOperator.listCollections(onlyNames);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
