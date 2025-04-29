package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.tapdata.tm.mcp.Utils.readJsonSchema;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
@Component
public class ListCollection extends MongoTool {

    public ListCollection(SessionAttribute sessionAttribute, UserService userService, DataSourceService dataSourceService) {
        super("listCollection", "List all collections in the MongoDB database",
                readJsonSchema("MongoListCollection.json"), sessionAttribute, userService, dataSourceService);
    }

    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        boolean nameOnly = Boolean.TRUE.equals(params.get("nameOnly"));

        try (MongoOperator mongoOperator = createMongoClient(exchange, params)){

            mongoOperator.connect();

            return makeCallToolResult(mongoOperator.listCollections(nameOnly));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
