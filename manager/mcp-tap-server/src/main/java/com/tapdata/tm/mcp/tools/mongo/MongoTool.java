package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.mcp.tools.Tool;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Map;

import static com.tapdata.tm.mcp.Utils.getStringValue;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/27 09:50
 */
public abstract class MongoTool extends Tool {
    private final DataSourceService dataSourceService;

    public MongoTool(String name, String description, String schema,
                     SessionAttribute sessionAttribute, UserService userService, DataSourceService dataSourceService) {
        super(name, description, schema, sessionAttribute, userService);
        this.dataSourceService = dataSourceService;
    }

    protected MongoOperator createMongoClient(McpSyncServerExchange exchange, Map<String, Object> params) {
        String connectionId = getStringValue(params, "connectionId");
        UserDetail userDetail = getUserDetail(exchange);
        if (StringUtils.isBlank(connectionId))
            throw new RuntimeException("Parameter connectionId is required.");

        DataSourceConnectionDto datasourceDto =
                dataSourceService.findOne(Query.query(Criteria.where("_id").is(toObjectId(connectionId))), userDetail);

        if (datasourceDto == null)
            throw new RuntimeException("Could not find datasource connection by id: " + connectionId);

        if (!"mongodb".equalsIgnoreCase(datasourceDto.getDatabase_type()))
            throw new RuntimeException("Only support MongoDB database to use this tool.");

        return new MongoOperator(datasourceDto);
    }
}
