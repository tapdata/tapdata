package com.tapdata.tm.mcp.resource;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/27 14:57
 */
@Component
public class ConnectionResource extends Resource{

    private final DataSourceService dataSourceService;

    public ConnectionResource(SessionAttribute sessionAttribute, UserService userService,
                              DataSourceService dataSourceService) {
        super("tap://connections", "Connections",
                "Database connections in TapData", "application/json", null,
                sessionAttribute, userService);
        this.dataSourceService = dataSourceService;
    }

    @Override
    public McpSchema.ReadResourceResult call(McpSyncServerExchange exchange, McpSchema.ReadResourceRequest request) {

        UserDetail userDetail = getUserDetail(exchange);

        String uri = request.uri();

        Pattern pattern = Pattern.compile("^tap:\\/\\/([a-z0-9]{24})(\\/([a-z0-9]{24}))?$");
        Matcher matcher = pattern.matcher(uri);
        String connectionId = null;
        String dataSchemaId = null;
        if (matcher.matches()) {
            if (matcher.groupCount() > 1) {
                connectionId = matcher.group(1);
            }
            if (matcher.groupCount() > 3) {
                dataSchemaId = matcher.group(3);
            }
        }

        List<McpSchema.ResourceContents> result = null;

        if (StringUtils.isNotBlank(dataSchemaId)) {
            String finalDataSchemaId = dataSchemaId;
            DataSourceConnectionDto datasource = dataSourceService.getById(toObjectId(connectionId), null, false, userDetail);
            if (datasource.getSchema() != null && datasource.getSchema().getTables() != null) {
                Table table = datasource.getSchema().getTables().stream().filter(t -> finalDataSchemaId.equals(t.getTableId())).findFirst().orElse(null);
                if (table != null) {
                    result = Collections.singletonList(new McpSchema.TextResourceContents(
                            String.format("tap://%s/%s", connectionId, dataSchemaId),
                            "application/json",
                            Utils.toJson(table)
                    ));
                }
            }
        } else {
            Criteria criteria = Criteria.where("status").is(DataSourceEntity.STATUS_READY)
                    .and("database_type").nin(Collections.singletonList("Dummy"));

            if (StringUtils.isNotBlank(connectionId)) {
                criteria.and("_id").is(toObjectId(connectionId));
            }
            result = dataSourceService.findAll(Query.query(criteria), userDetail)
                    .stream()
                    .map(ds -> {
                        Map<String, Object> data = Utils.readConnection(ds);

                        return new McpSchema.TextResourceContents(String.format("tap://%s", ds.getId().toHexString()), "application/json", Utils.toJson(data));
                    }).collect(Collectors.toList());
        }

        return new McpSchema.ReadResourceResult(result);
    }
}
