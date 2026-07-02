package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.mcp.tools.McpToolSupport;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Component
public class MongoOperatorFactory {

    private final McpToolSupport toolSupport;
    private final DataSourceService dataSourceService;

    public MongoOperatorFactory(McpToolSupport toolSupport, DataSourceService dataSourceService) {
        this.toolSupport = toolSupport;
        this.dataSourceService = dataSourceService;
    }

    public MongoOperator create(McpSyncRequestContext context, String connectionId) {
        UserDetail userDetail = toolSupport.getUserDetail(context);
        if (StringUtils.isBlank(connectionId)) {
            throw new RuntimeException("Parameter connectionId is required.");
        }

        DataSourceConnectionDto datasourceDto =
                dataSourceService.findOne(Query.query(Criteria.where("_id").is(toObjectId(connectionId))), userDetail);

        if (datasourceDto == null) {
            throw new RuntimeException("Could not find datasource connection by id: " + connectionId);
        }

        if (!"mongodb".equalsIgnoreCase(datasourceDto.getDatabase_type())) {
            throw new RuntimeException("Only support MongoDB database to use this tool.");
        }

        return new MongoOperator(datasourceDto);
    }
}
