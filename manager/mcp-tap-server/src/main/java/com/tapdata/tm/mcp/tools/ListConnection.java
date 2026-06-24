package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
@Component
public class ListConnection {

    private final McpToolSupport toolSupport;
    private final DataSourceService dataSourceService;

    public ListConnection(McpToolSupport toolSupport, DataSourceService dataSourceService) {
        this.toolSupport = toolSupport;
        this.dataSourceService = dataSourceService;
    }

    @McpTool(name = "listConnection", description = "List all available database connections in TapData")
    public List<Map<String, Object>> listConnection(
            McpSyncRequestContext context,
            @McpToolParam(required = false, description = "Optional case-insensitive connection name keyword.") String name) {
        UserDetail userDetail = toolSupport.getUserDetail(context);

        Criteria criteria = Criteria.where("status").is(DataSourceEntity.STATUS_READY)
                .and("database_type").nin(Collections.singletonList("Dummy"));
        if (name != null) {
            criteria.and("name").regex(name, "i");
        }

        List<Map<String, Object>> result = dataSourceService.findAll(Query.query(criteria), userDetail)
                .stream().map(Utils::readConnection).collect(Collectors.toList());

        return result;
    }
}
