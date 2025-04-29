package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static com.tapdata.tm.mcp.Utils.getStringValue;
import static com.tapdata.tm.mcp.Utils.readJsonSchema;
import static java.util.Collections.emptyList;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 19:24
 */
@Component
public class Aggregate extends MongoTool {

    public Aggregate(SessionAttribute sessionAttribute, UserService userService, DataSourceService dataSourceService) {
        super("aggregate", "Execute a MongoDB aggregation pipeline process multiple documents and return computed results",
                readJsonSchema("MongoAggregate.json"), sessionAttribute, userService, dataSourceService);
    }

    @Override
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        String collectionName = getStringValue(params, "collectionName");
        if (StringUtils.isBlank(collectionName)) {
            throw new RuntimeException("Parameter collectionName is required");
        }
        try (MongoOperator mongoOperator = createMongoClient(exchange, params)){

            mongoOperator.connect();
            List<Document> data = mongoOperator.aggregate(collectionName, params);
            return makeCallToolResult(data);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
