package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceEnum;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.tapdata.tm.mcp.Utils.getStringValue;

/**
 * MCP tool for creating a database connection in TapData.
 * Currently, supports MongoDB, MySQL, Oracle, SQL Server and PostgreSQL.
 * <p>
 * The data source type is resolved from a fuzzy user description against the data source
 * definitions. When 'config' is omitted the tool returns the resolved type together with the
 * connection configuration schema (the fields to fill) instead of creating anything.
 */
@Slf4j
@Component
public class CreateConnection extends Tool {

    private final DataSourceService dataSourceService;
    private final DataSourceDefinitionService dataSourceDefinitionService;

    /** Supported data source types and the normalized tokens used to match user input. */
    private static final Map<String, List<String>> SUPPORTED = new LinkedHashMap<>();

    static {
        SUPPORTED.put("MongoDB", Arrays.asList("mongodb", "mongo"));
        SUPPORTED.put("MySQL", Collections.singletonList("mysql"));
        SUPPORTED.put("Oracle", Collections.singletonList("oracle"));
        SUPPORTED.put("SQL Server", Arrays.asList("sqlserver", "mssql", "sqlsvr", "microsoftsqlserver"));
        SUPPORTED.put("PostgreSQL", Arrays.asList("postgresql", "postgres", "pg", "pgsql"));
    }

    public CreateConnection(SessionAttribute sessionAttribute, UserService userService,
                            DataSourceService dataSourceService,
                            DataSourceDefinitionService dataSourceDefinitionService) {
        super("createConnection",
                "Create a database connection in TapData. Currently supports MongoDB, MySQL, Oracle, SQL Server and PostgreSQL. " +
                        "It resolves the data source type from a fuzzy user description, then creates the connection with the given configuration. " +
                        "If 'config' is omitted, it returns the resolved data source type and its connection configuration schema " +
                        "(the list of fields you need to fill) without creating anything; call again with 'config' filled to actually create the connection.",
                Utils.readJsonSchema("CreateConnection.json"), sessionAttribute, userService);
        this.dataSourceService = dataSourceService;
        this.dataSourceDefinitionService = dataSourceDefinitionService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {
        UserDetail userDetail = getUserDetail(exchange);

        String dataSourceType = getStringValue(params, "dataSourceType");
        String connectionName = getStringValue(params, "connectionName");
        String connectionType = getStringValue(params, "connectionType", "source_and_target");
        Map<String, Object> config = params.get("config") instanceof Map ? (Map<String, Object>) params.get("config") : null;

        if (StringUtils.isBlank(dataSourceType)) {
            throw new RuntimeException("Parameter dataSourceType is required.");
        }

        DataSourceTypeDto typeDto = resolveType(dataSourceType, userDetail);
        if (typeDto == null) {
            throw new RuntimeException("Unsupported or unrecognized data source type: '" + dataSourceType +
                    "'. Currently supported types: " + String.join(", ", SUPPORTED.keySet()) + ".");
        }

        // Discovery mode: no config provided, return the connection configuration schema as guidance.
        if (config == null || config.isEmpty()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("resolvedType", buildTypeInfo(typeDto));
            result.put("configFields", extractConfigFields(typeDto));
            result.put("message", "No 'config' provided. Above is the resolved data source type and its connection configuration fields. " +
                    "Fill the 'config' object with the appropriate field values and call createConnection again to create the connection.");
            return makeCallToolResult(result);
        }

        if (StringUtils.isBlank(connectionName)) {
            throw new RuntimeException("Parameter connectionName is required to create a connection.");
        }

        // For MongoDB, a non-blank 'uri' means URI mode, so 'isUri' must be true.
        if (DataSourceEnum.isMongoDB(typeDto.getType())
                && StringUtils.isNotBlank(getStringValue(config, "uri"))
                && !config.containsKey("isUri")) {
            config.put("isUri", true);
        }

        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        connection.setId(null);
        connection.setName(connectionName);
        connection.setConnection_type(connectionType);
        connection.setDatabase_type(typeDto.getType());
        connection.setConfig(config);
        connection.setCreateType(CreateTypeEnum.User);
        connection.setSubmit(true);
        connection.setUpdateSchema(true);

        DataSourceConnectionDto created = dataSourceService.addConnection(connection, userDetail);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", created.getId().toHexString());
        result.put("name", created.getName());
        result.put("databaseType", created.getDatabase_type());
        result.put("connectionType", created.getConnection_type());
        result.put("status", created.getStatus());
        result.put("message", "Connection created successfully. TapData is testing the connection and loading the schema in the background. " +
                "Use listConnection to check its status.");
        return makeCallToolResult(result);
    }

    private Map<String, Object> buildTypeInfo(DataSourceTypeDto typeDto) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", typeDto.getName());
        info.put("realName", typeDto.getRealName());
        info.put("type", typeDto.getType());
        info.put("pdkId", typeDto.getPdkId());
        info.put("connectionType", typeDto.getConnectionType());
        return info;
    }

    /**
     * Fuzzy-resolve the user described data source type to one of the supported {@link DataSourceTypeDto}.
     * Returns null when the input does not map to any supported type or no matching definition is found.
     */
    private DataSourceTypeDto resolveType(String input, UserDetail userDetail) {
        String norm = normalize(input);
        List<String> tokens = null;
        for (Map.Entry<String, List<String>> entry : SUPPORTED.entrySet()) {
            if (matchesTokens(norm, entry.getValue())) {
                tokens = entry.getValue();
                break;
            }
        }
        if (tokens == null) {
            return null;
        }

        List<DataSourceTypeDto> types = fetchTypes(userDetail);
        // 1) Exact match first so that base types win over variants such as "Aliyun MongoDB" /
        //    "Tencent MongoDB" when the user asks for "MongoDB". Prefer pdkId, then type/name/realName.
        for (String token : tokens) {
            for (DataSourceTypeDto t : types) {
                if (equalsNorm(t.getPdkId(), token)) {
                    return t;
                }
            }
            for (DataSourceTypeDto t : types) {
                if (equalsNorm(t.getType(), token) || equalsNorm(t.getName(), token) || equalsNorm(t.getRealName(), token)) {
                    return t;
                }
            }
        }
        // 2) Fall back to partial match only when no exact match exists.
        for (DataSourceTypeDto t : types) {
            if (matches(t.getPdkId(), tokens)) {
                return t;
            }
        }
        for (DataSourceTypeDto t : types) {
            if (matches(t.getType(), tokens) || matches(t.getName(), tokens) || matches(t.getRealName(), tokens)) {
                return t;
            }
        }
        return null;
    }

    private boolean equalsNorm(String value, String token) {
        return StringUtils.isNotBlank(value) && normalize(value).equals(token);
    }

    private List<DataSourceTypeDto> fetchTypes(UserDetail userDetail) {
        Filter filter = new Filter();
        Field field = new Field();
        field.put("pdkId", true);
        field.put("name", true);
        field.put("realName", true);
        field.put("type", true);
        field.put("connectionType", true);
        field.put("properties", true);
        filter.setFields(field);
        List<DataSourceTypeDto> types = dataSourceDefinitionService.dataSourceTypes(userDetail, filter);
        return types == null ? Collections.emptyList() : types;
    }

    private boolean matches(String value, List<String> tokens) {
        return StringUtils.isNotBlank(value) && matchesTokens(normalize(value), tokens);
    }

    private boolean matchesTokens(String norm, List<String> tokens) {
        if (StringUtils.isBlank(norm)) {
            return false;
        }
        for (String t : tokens) {
            if (norm.equals(t) || norm.contains(t) || t.contains(norm)) {
                return true;
            }
        }
        return false;
    }

    private String normalize(String s) {
        return s == null ? "" : s.toLowerCase().replaceAll("[^a-z0-9]", "");
    }

    /**
     * Extract the connection configuration fields from {@code properties.connection.properties} of
     * the data source definition (a Formily schema). Nested containers such as OPTIONAL_FIELDS are
     * flattened and their fields are marked as advanced. Pure display-only (type=void) fields are skipped.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractConfigFields(DataSourceTypeDto typeDto) {
        List<Map<String, Object>> out = new ArrayList<>();
        Map<String, Object> props = typeDto.getProperties();
        if (props == null) {
            return out;
        }
        Object connection = props.get("connection");
        if (!(connection instanceof Map)) {
            return out;
        }
        Object connProps = ((Map<String, Object>) connection).get("properties");
        if (!(connProps instanceof Map)) {
            return out;
        }
        flatten((Map<String, Object>) connProps, false, out);
        out.sort(Comparator.comparingInt(m -> ((Number) m.getOrDefault("index", Integer.MAX_VALUE)).intValue()));
        return out;
    }

    @SuppressWarnings("unchecked")
    private void flatten(Map<String, Object> props, boolean advanced, List<Map<String, Object>> out) {
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                continue;
            }
            String key = entry.getKey();
            Map<String, Object> fieldSchema = (Map<String, Object>) entry.getValue();
            Object type = fieldSchema.get("type");
            Object nested = fieldSchema.get("properties");
            boolean adv = advanced || "OPTIONAL_FIELDS".equals(key);
            if (nested instanceof Map) {
                flatten((Map<String, Object>) nested, adv, out);
                continue;
            }
            // Skip pure display-only fields (tips, separators, etc.).
            if ("void".equals(type)) {
                continue;
            }
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", key);
            if (fieldSchema.get("title") != null) {
                info.put("title", fieldSchema.get("title"));
            }
            if (type != null) {
                info.put("type", type);
            }
            if (fieldSchema.get("x-component") != null) {
                info.put("component", fieldSchema.get("x-component"));
            }
            info.put("required", Boolean.TRUE.equals(fieldSchema.get("required")));
            info.put("advanced", adv);
            if (fieldSchema.get("default") != null) {
                info.put("default", fieldSchema.get("default"));
            }
            if (fieldSchema.get("enum") != null) {
                info.put("enum", fieldSchema.get("enum"));
            }
            if (fieldSchema.get("x-index") != null) {
                info.put("index", fieldSchema.get("x-index"));
            }
            out.add(info);
        }
    }
}
