package com.tapdata.tm.mcp.resource;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.mcp.tools.McpToolSupport;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.mcp.annotation.McpResource;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/27 14:57
 */
@Component
public class ConnectionResource {

    private static final String CONNECTIONS_URI = "tap://connections";
    private static final Pattern CONNECTION_URI =
            Pattern.compile("^tap:\\/\\/([a-z0-9]{24})(\\/([a-z0-9]{24}))?$");

    private final McpToolSupport toolSupport;
    private final DataSourceService dataSourceService;

    public ConnectionResource(McpToolSupport toolSupport, DataSourceService dataSourceService) {
        this.toolSupport = toolSupport;
        this.dataSourceService = dataSourceService;
    }

    @McpResource(name = "Connections", uri = "tap://connections", description = "Database connections in TapData", mimeType = "application/json")
    public String listConnections(McpSyncRequestContext context) {
        return read(context, CONNECTIONS_URI);
    }

    @McpResource(name = "Connection", uri = "tap://{connectionId}", description = "A TapData database connection", mimeType = "application/json")
    public String getConnection(McpSyncRequestContext context, String connectionId) {
        return read(context, String.format("tap://%s", connectionId));
    }

    @McpResource(name = "DataModel", uri = "tap://{connectionId}/{dataModelId}", description = "A data model loaded by a TapData connection", mimeType = "application/json")
    public String getDataModel(McpSyncRequestContext context, String connectionId, String dataModelId) {
        return read(context, String.format("tap://%s/%s", connectionId, dataModelId));
    }

    String read(McpSyncRequestContext context, String uri) {
        ResourceUri resourceUri = ResourceUri.parse(uri);
        if (resourceUri == null) {
            return null;
        }

        UserDetail userDetail = toolSupport.getUserDetail(context);
        Object result = StringUtils.isNotBlank(resourceUri.dataSchemaId)
                ? readDataModel(userDetail, resourceUri.connectionId, resourceUri.dataSchemaId)
                : readConnections(userDetail, resourceUri.connectionId);

        return result == null ? null : Utils.toJson(result);
    }

    private Object readDataModel(UserDetail userDetail, String connectionId, String dataSchemaId) {
        DataSourceConnectionDto datasource = dataSourceService.getById(toObjectId(connectionId), null, false, userDetail);
        if (datasource == null || datasource.getSchema() == null || datasource.getSchema().getTables() == null) {
            return null;
        }
        return datasource.getSchema().getTables()
                .stream()
                .filter(t -> dataSchemaId.equals(t.getTableId()))
                .findFirst()
                .orElse(null);
    }

    private Object readConnections(UserDetail userDetail, String connectionId) {
        Criteria criteria = Criteria.where("status").is(DataSourceEntity.STATUS_READY)
                .and("database_type").nin(Collections.singletonList("Dummy"));

        if (StringUtils.isNotBlank(connectionId)) {
            criteria.and("_id").is(toObjectId(connectionId));
        }

        List<Map<String, Object>> connections = Optional.ofNullable(dataSourceService.findAll(Query.query(criteria), userDetail))
                .orElseGet(ArrayList::new)
                .stream()
                .map(Utils::readConnection)
                .collect(Collectors.toList());

        if (StringUtils.isNotBlank(connectionId)) {
            return connections.stream().findFirst().orElse(null);
        }
        return connections;
    }

    private static class ResourceUri {
        private final String connectionId;
        private final String dataSchemaId;

        private ResourceUri(String connectionId, String dataSchemaId) {
            this.connectionId = connectionId;
            this.dataSchemaId = dataSchemaId;
        }

        private static ResourceUri parse(String uri) {
            if (CONNECTIONS_URI.equals(uri)) {
                return new ResourceUri(null, null);
            }

            Matcher matcher = CONNECTION_URI.matcher(uri);
            if (!matcher.matches()) {
                return null;
            }
            return new ResourceUri(matcher.group(1), matcher.group(3));
        }
    }
}
