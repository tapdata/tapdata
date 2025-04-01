package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import javax.naming.AuthenticationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tapdata.tm.mcp.Utils.*;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
@Component
public class ListConnection extends Tool{

    private final DataSourceService dataSourceService;

    public ListConnection(SessionAttribute sessionAttribute, DataSourceService dataSourceService, UserService userService) {
        super("listConnection", "List all available database connections in TapData",
                readJsonSchema("ListConnection.json"), sessionAttribute, userService);
        this.dataSourceService = dataSourceService;
    }

    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        UserDetail userDetail = getUserDetail(exchange);
        String name = getStringValue(params, "name");

        Criteria criteria = Criteria.where("status").is(DataSourceEntity.STATUS_READY)
                .and("database_type").nin(Collections.singletonList("Dummy"));
        if (name != null) {
            criteria.and("name").regex(name, "i");
        }

        List<Map<String, Object>> result = dataSourceService.findAll(Query.query(criteria), userDetail)
                .stream().map(Utils::readConnection).collect(Collectors.toList());

        return makeCallToolResult(result);
    }
}
