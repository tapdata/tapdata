package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.mcp.Utils.*;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
@Component
public class ListDataModel extends Tool{

    private final DataSourceService dataSourceService;

    public ListDataModel(SessionAttribute sessionAttribute, DataSourceService dataSourceService, UserService userService) {
        super("listDataModel", "List all data model loaded by connection in TapData",
                readJsonSchema("ListDataModel.json"), sessionAttribute, userService);
        this.dataSourceService = dataSourceService;
    }

    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        UserDetail userDetail = getUserDetail(exchange);
        String connectionId = getStringValue(params, "connectionId");
        boolean includeFields = Boolean.TRUE.equals(params.get("includeFields"));
        String name = getStringValue(params, "name");

        if (StringUtils.isBlank(connectionId))
            throw new RuntimeException("Parameter connectionId is required.");

        DataSourceConnectionDto connection = dataSourceService.getById(toObjectId(connectionId), null, false, userDetail);
        Schema schema = connection.getSchema();
        List<Map<String, Object>> result =
                schema.getTables().stream()
                        .filter(table -> name == null || table.getTableName().toLowerCase().contains(name.toLowerCase()))
                        .map(table -> {
            Map<String, Object> data = new HashMap<>();
            data.put("id", table.getTableId());
            data.put("type", table.getMetaType());
            data.put("name", table.getTableName());
            data.put("collectionName", table.getTableName());
            if(includeFields) {
                data.put("fields", table.getFields().stream().map(field -> {
                    Map<String, Object> f = new HashMap<>();
                    f.put("name", field.getFieldName());
                    f.put("type", field.getDataType());
                    f.put("unique", field.isUnique());
                    f.put("primaryKey", field.getPrimaryKey());
                    return f;
                }).collect(Collectors.toList()));
                data.put("indexes", table.getIndices().stream().map(index -> {
                    Map<String, Object> f = new HashMap<>();
                    f.put("name", index.getIndexName());
                    f.put("unique", index.isUnique());
                    f.put("keys", index.getColumns().stream().map(i -> {
                        if (i != null && table.getFields().size() > i.getColumnPosition()) {
                            return table.getFields().get(i.getColumnPosition()).getFieldName();
                        }
                        return null;
                    }).filter(Objects::nonNull).collect(Collectors.toList()));
                    return f;
                }).collect(Collectors.toList()));
            }
            return data;
        }).collect(Collectors.toList());

        return makeCallToolResult(result);
    }
}
