package com.tapdata.tm.mcp.tools.mongo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.mcp.tools.Tool;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.tapdata.tm.mcp.Utils.*;
import static java.util.Collections.emptyList;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 19:24
 */
@Component
public class Count extends MongoTool {

    public Count(SessionAttribute sessionAttribute, UserService userService, DataSourceService dataSourceService) {
        super("count", "Query the number of rows in a MongoDB Collection that match the filter criteria",
                readJsonSchema("MongoCount.json"), sessionAttribute, userService, dataSourceService);
    }

    @Override
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        String collectionName = getStringValue(params, "collectionName");
        if (StringUtils.isBlank(collectionName)) {
            throw new RuntimeException("Parameter collectionName is required");
        }

        try (MongoOperator mongoOperator = createMongoClient(exchange, params)){

            mongoOperator.connect();
            long count = mongoOperator.count(collectionName, params);

            Map<String, Object> result = new HashMap<>();
            result.put("count", count);
            result.put("ok", 1);

            return makeCallToolResult(result);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
