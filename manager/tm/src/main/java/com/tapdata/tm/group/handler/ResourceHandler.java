package com.tapdata.tm.group.handler;

import cn.hutool.extra.spring.SpringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.utils.ExcelUtil;
import com.tapdata.tm.utils.MongoUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.util.*;

/**
 * 资源处理器接口
 * 定义分组模块中各种资源类型的统一处理规范
 * 支持资源的加载、导出、导入、校验等操作
 *
 */
public interface ResourceHandler {

    Logger log = LoggerFactory.getLogger(ResourceHandler.class);

    /**
     * 需要脱敏的标准化 apiServerKey 集合，跨所有数据源一致
     */
    Set<String> SENSITIVE_API_KEYS = Set.of(
            "database_host",
            "database_port",
            "database_username",
            "database_password",
            "database_uri"
    );

    /**
     * vault.json 字段后缀 -> 标准化 apiServerKey 的映射
     * 对应 vault key 格式：{connectionName}_{suffix}
     */
    Map<String, String> VAULT_SUFFIX_TO_API_KEY = Map.of(
            "host",     "database_host",
            "port",     "database_port",
            "user",     "database_username",
            "password", "database_password",
            "uri",      "database_uri"
    );

    /**
     * 当 schema BFS 无法找到 apiServerKey 映射时，使用 fallback 默认 config key。
     * vault suffix -> 默认 config key（直接写入 config 根级别）
     */
    Map<String, String> VAULT_SUFFIX_FALLBACK_CONFIG_KEY = Map.of(
            "host",     "host",
            "port",     "port",
            "user",     "username",
            "password", "password"
    );

    /**
     * 获取当前处理器支持的资源类型
     *
     * @return 资源类型
     */
    ResourceType getResourceType();

    /**
     * 根据 ID 列表加载资源
     *
     * @param ids  资源 ID 列表
     * @param user 用户信息
     * @return 资源对象列表
     */
    List<?> loadResources(List<String> ids, UserDetail user);

    /**
     * 构建资源的导出数据
     *
     * @param resources 资源列表
     * @param user      用户信息
     * @return 导出数据 payload 列表
     */
    List<TaskUpAndLoadDto> buildExportPayload(List<?> resources, UserDetail user);

    /**
     * 从导入的 payload 中收集资源和元数据
     *
     * @param payload      导入的 payload 列表
     * @param resourceMap  资源映射表（key 为资源 ID 或 name，value 为资源对象）
     * @param metadataList 元数据列表
     */
    void collectPayload(List<TaskUpAndLoadDto> payload, Map<String, ?> resourceMap,
            List<MetadataInstancesDto> metadataList);

    /**
     * 从资源中收集连接
     *
     * @param resources 资源列表
     */
    List<DataSourceEntity> loadConnections(List<?> resources);

    /**
     * 查找重名的资源
     *
     * @param resources 待检查的资源列表
     * @param user      用户信息
     * @return 重名资源映射（key 为资源名称，value 为重复标记）
     */
    Map<String, String> findDuplicateNames(Iterable<?> resources, UserDetail user);

    /**
     * 解析资源名称
     *
     * @param resourceId  资源 ID
     * @param resourceMap 资源映射表
     * @return 资源名称，如果未找到返回 null
     */
    String resolveResourceName(String resourceId, Map<String, ?> resourceMap);

    default void handleRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloadsByType, List<?> resources,
            UserDetail user,Set<ObjectId> tagIds) {
        List<DataSourceEntity> connections = loadConnections(resources);
        List<TaskUpAndLoadDto> connectionPayload = buildConnectionPayload(connections, user);
        if (CollectionUtils.isNotEmpty(connections)) {
            connections.forEach(c -> {
                if (CollectionUtils.isNotEmpty(c.getListtags())) {
                    tagIds.addAll(c.getListtags().stream().map(t -> MongoUtils.toObjectId(t.get("id"))).toList());
                }
            });
        }
        payloadsByType.computeIfAbsent(ResourceType.CONNECTION.name(), k -> new ArrayList<>())
                .addAll(connectionPayload);
    }

    /**
     * 构建连接的导出payload（只收集连接和元数据，不生成Excel）
     */
    default List<TaskUpAndLoadDto> buildConnectionPayload(List<DataSourceEntity> connections, UserDetail user) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        if (CollectionUtils.isEmpty(connections)) {
            return payload;
        }

        for (DataSourceEntity entity : connections) {
            entity.setConnectionString(null);
            entity.setCustomId(null);
            entity.setLastUpdBy(null);
            // 移除环境相关字段，避免跨环境导入时产生误差
            if (entity.getConfig() != null) {
                entity.getConfig().remove("datasourceInstanceId");
            }
            DataSourceDefinitionService dataSourceDefinitionService = SpringUtil
                    .getBean(DataSourceDefinitionService.class);
            DataSourceDefinitionDto definition = dataSourceDefinitionService
                    .findByPdkHash(entity.getPdkHash(), Integer.MAX_VALUE, user);
            if (definition != null) {
                maskSensitiveConfigFields(entity, definition);
            }

            // 收集元数据
            String id = entity.getId().toHexString();
            String databaseQualifiedName;
            if (DataSourceDefinitionDto.PDK_TYPE.equals(entity.getPdkType())) {
                databaseQualifiedName = MetaDataBuilderUtils.generatePdkQualifiedName(
                        "database", id, null, entity.getDefinitionPdkId(),
                        entity.getDefinitionGroup(), entity.getDefinitionVersion(), null);
            } else {
                databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", id, null);
            }
            MetadataInstancesService metadataInstancesService = SpringUtil.getBean(MetadataInstancesService.class);
            MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
                    Query.query(Criteria.where("qualified_name").is(databaseQualifiedName).and("is_deleted").ne(true)),
                    user);
            if (dataSourceMetadataInstance != null) {
                payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_METADATA_INSTANCES,
                        JsonUtil.toJsonUseJackson(dataSourceMetadataInstance)));
            }

            // 序列化连接数据，并补充 definitionPdkAPIVersion 供导入端版本校验使用
            Map<String, Object> connMap = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(entity), Map.class);
            if (connMap != null) {
                // Entity.id 无 @JsonSerialize 注解，Jackson 默认只序列化 getTimestamp() 导致 id 格式不完整，
                // 显式写成 hex string 确保导入端 ObjectIdDeserialize 能正确还原
                if (entity.getId() != null) {
                    connMap.put("id", entity.getId().toHexString());
                }
                if (definition != null) {
                    connMap.put("definitionPdkAPIVersion", definition.getPdkAPIVersion());
                }
                // Entity 序列化 userId 为 "userId"，但 DTO @JsonProperty 期望 "user_id"
                // 统一为 DTO 字段名，确保导入反序列化时能正确还原
                if (connMap.containsKey("userId") && !connMap.containsKey("user_id")) {
                    connMap.put("user_id", connMap.remove("userId"));
                }
            }
            payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_CONNECTION,
                    JsonUtil.toJsonUseJackson(connMap != null ? connMap : entity)));
        }

        return payload;
    }



    default void collectPayloadRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloads,
            Map<ResourceType, Map<String, ?>> resourceMap,
            Map<ResourceType, List<MetadataInstancesDto>> metadataList,UserDetail user) {
        if (MapUtils.isEmpty(payloads)) {
            return;
        }
        Map<String, Object> connections = (Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.CONNECTION,
                k -> new HashMap<>());
        List<MetadataInstancesDto> connectionMetadata = metadataList.computeIfAbsent(ResourceType.CONNECTION,
                k -> new ArrayList<>());

        // 处理Excel文件（Connections.xlsx）
        List<TaskUpAndLoadDto> excelPayload = payloads.getOrDefault(GroupConstants.COLLECTION_CONNECTION_EXCEL, Collections.emptyList());
        for (TaskUpAndLoadDto taskUpAndLoadDto : excelPayload) {
            byte[] binaryData = taskUpAndLoadDto.getBinaryData();
            if (binaryData != null && binaryData.length > 0) {
                try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(binaryData)) {
                    List<DataSourceConnectionDto> importedConnections = ExcelUtil.importConnectionsFromExcel(bais,user);
                    for (DataSourceConnectionDto connectionDto : importedConnections) {
                        if (connectionDto != null && connectionDto.getId() != null) {
                            connections.putIfAbsent(connectionDto.getId().toHexString(), connectionDto);
                        } else if (connectionDto != null) {
                            log.warn("Connection from Excel has no _id, skip: name={}", connectionDto.getName());
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to import connections from Excel", e);
                }
            }
        }

        // 处理JSON文件（Connection.json）
        String filename = ResourceType.getResourceName(ResourceType.CONNECTION.name());
        List<TaskUpAndLoadDto> jsonPayload = payloads.getOrDefault(filename, Collections.emptyList());
        for (TaskUpAndLoadDto taskUpAndLoadDto : jsonPayload) {
            if (StringUtils.isBlank(taskUpAndLoadDto.getJson())) {
                continue;
            }
            if (GroupConstants.COLLECTION_CONNECTION.equals(taskUpAndLoadDto.getCollectionName())) {
                DataSourceConnectionDto connectionDto = JsonUtil.parseJsonUseJackson(taskUpAndLoadDto.getJson(),
                        DataSourceConnectionDto.class);
                if (connectionDto != null) {
                    // 兼容旧导出文件：Entity 序列化字段名 "userId" 与 DTO @JsonProperty("user_id") 不一致，
                    // 旧文件中 Jackson 反序列化时无法匹配导致 userId 为 null，此处从原始 JSON 中补回
                    if (connectionDto.getUserId() == null) {
                        Map<String, Object> rawMap = JsonUtil.parseJsonUseJackson(taskUpAndLoadDto.getJson(), Map.class);
                        if (rawMap != null && rawMap.get("userId") instanceof String) {
                            connectionDto.setUserId((String) rawMap.get("userId"));
                        }
                    }
                    if (connectionDto.getId() == null) {
                        log.warn("Connection from JSON has no _id, skip: name={}", connectionDto.getName());
                    } else {
                        connections.putIfAbsent(connectionDto.getId().toHexString(), connectionDto);
                    }
                }
            } else if (GroupConstants.COLLECTION_METADATA_INSTANCES.equals(taskUpAndLoadDto.getCollectionName())) {
                MetadataInstancesDto metadataInstancesDto = JsonUtil.parseJsonUseJackson(taskUpAndLoadDto.getJson(),
                        MetadataInstancesDto.class);
                if (metadataInstancesDto != null) {
                    connectionMetadata.add(metadataInstancesDto);
                }
            }
        }
        Map<String, Object> metadataDefinitions = (Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.METADATA_DEFINITION,
                k -> new HashMap<>());
        //
        String tagFilename = ResourceType.getResourceName(ResourceType.METADATA_DEFINITION.name());
        List<TaskUpAndLoadDto> tagJsonPayload = payloads.getOrDefault(tagFilename, Collections.emptyList());
        for (TaskUpAndLoadDto taskUpAndLoadDto : tagJsonPayload) {
            if (StringUtils.isBlank(taskUpAndLoadDto.getJson())) {
                continue;
            }
            if (GroupConstants.METADATA_DEFINITION.equals(taskUpAndLoadDto.getCollectionName())) {
                MetadataDefinitionDto metadataDefinitionDto = JsonUtil.parseJsonUseJackson(taskUpAndLoadDto.getJson(),
                        MetadataDefinitionDto.class);
                if (metadataDefinitionDto != null) {
                    if (metadataDefinitionDto.getId() != null) {
                        metadataDefinitions.putIfAbsent(metadataDefinitionDto.getId().toHexString(), metadataDefinitionDto);
                    } else {
                        log.warn("MetadataDefinition has no _id, skip: value={}", metadataDefinitionDto.getValue());
                    }
                }
            }

        }
    }

    /**
     * BFS 遍历 definition.properties.connection.properties，
     * 返回 configPath -> apiServerKey 映射。
     * 如果 definition 为 null 或 schema 结构不匹配则返回空 map。
     */
    static Map<String, String> buildConfigPathToApiKeyMap(DataSourceDefinitionDto definition) {
        Map<String, String> result = new LinkedHashMap<>();
        Map<String, Object> connectionProperties = getConnectionProperties(definition);
        if (connectionProperties == null) return result;

        Deque<Object[]> queue = new ArrayDeque<>();
        queue.add(new Object[]{connectionProperties, ""});
        while (!queue.isEmpty()) {
            Object[] node = queue.poll();
            Map<String, Object> props = (Map<String, Object>) node[0];
            String prefix = (String) node[1];
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                if (!(entry.getValue() instanceof Map)) continue;
                Map<String, Object> meta = (Map<String, Object>) entry.getValue();
                String configPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                Object apiServerKey = meta.get("apiServerKey");
                if (apiServerKey instanceof String) {
                    result.put(configPath, (String) apiServerKey);
                }
                Object childProperties = meta.get("properties");
                if (childProperties instanceof Map) {
                    queue.add(new Object[]{childProperties, configPath});
                }
            }
        }
        return result;
    }

    /**
     * BFS 遍历 definition.properties.connection.properties，
     * 返回 configPath -> spec.json title 映射（英文标签）。
     */
    static Map<String, String> buildConfigPathToLabelMap(DataSourceDefinitionDto definition) {
        Map<String, String> result = new LinkedHashMap<>();
        Map<String, Object> connectionProperties = getConnectionProperties(definition);
        if (connectionProperties == null) return result;

        // Prepare English translation map from definition messages
        Map<String, Object> enMessages = null;
        if (definition.getMessages() != null) {
            Object langMap = definition.getMessages().get("en_US");
            if (langMap instanceof Map) {
                enMessages = (Map<String, Object>) langMap;
            }
        }

        Deque<Object[]> queue = new ArrayDeque<>();
        queue.add(new Object[]{connectionProperties, ""});
        while (!queue.isEmpty()) {
            Object[] node = queue.poll();
            Map<String, Object> props = (Map<String, Object>) node[0];
            String prefix = (String) node[1];
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                if (!(entry.getValue() instanceof Map)) continue;
                Map<String, Object> meta = (Map<String, Object>) entry.getValue();
                String configPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                Object title = meta.get("title");
                if (title instanceof String && !((String) title).isBlank()) {
                    String resolved = resolveI18nPlaceholder((String) title, enMessages);
                    if (resolved != null) {
                        result.put(configPath, resolved);
                    }
                }
                Object childProperties = meta.get("properties");
                if (childProperties instanceof Map) {
                    queue.add(new Object[]{childProperties, configPath});
                }
            }
        }
        return result;
    }

    /**
     * Resolve ${key} placeholders in a title string using the provided translation map.
     * Returns null if the placeholder cannot be resolved (translation missing).
     */
    private static String resolveI18nPlaceholder(String title, Map<String, Object> langMessages) {
        if (title == null || !title.contains("${")) {
            return title;
        }
        if (langMessages == null) {
            return null;
        }
        String resolved = title;
        int start;
        while ((start = resolved.indexOf("${")) >= 0) {
            int end = resolved.indexOf("}", start);
            if (end < 0) break;
            String key = resolved.substring(start + 2, end);
            Object value = langMessages.get(key);
            if (value instanceof String) {
                resolved = resolved.substring(0, start) + value + resolved.substring(end + 1);
            } else {
                return null;
            }
        }
        return resolved;
    }

    /**
     * 从 definition 中提取 connection.properties 节点，BFS 工具方法的公共前置。
     */
    private static Map<String, Object> getConnectionProperties(DataSourceDefinitionDto definition) {
        if (definition == null) return null;
        LinkedHashMap<String, Object> properties = definition.getProperties();
        if (properties == null) return null;
        Object connection = properties.get("connection");
        if (!(connection instanceof Map)) return null;
        Object connectionProperties = ((Map<?, ?>) connection).get("properties");
        return (connectionProperties instanceof Map) ? (Map<String, Object>) connectionProperties : null;
    }

    /**
     * 返回 definition 中标记为敏感的 config path 集合（apiServerKey 在 SENSITIVE_API_KEYS 中的路径）。
     */
    static Set<String> getMaskedConfigPaths(DataSourceDefinitionDto definition) {
        Set<String> paths = new HashSet<>();
        Map<String, String> pathToApiKey = buildConfigPathToApiKeyMap(definition);
        for (Map.Entry<String, String> entry : pathToApiKey.entrySet()) {
            if (SENSITIVE_API_KEYS.contains(entry.getValue())) {
                paths.add(entry.getKey());
            }
        }
        return paths;
    }

    /**
     * 根据 DataSourceDefinitionDto 中的 apiServerKey 定义，找到 DataSourceEntity.config
     * 里对应的路径并清空值。只处理 SENSITIVE_API_KEYS 中声明的标准化 apiServerKey。
     */
    private static void maskSensitiveConfigFields(DataSourceEntity conn,
            DataSourceDefinitionDto definition) {
        Map<String, Object> config = conn.getConfig();
        if (MapUtils.isEmpty(config) || definition == null) {
            return;
        }
        Set<String> maskedPaths = getMaskedConfigPaths(definition);
        for (String path : maskedPaths) {
            removeNestedValue(config, path);
        }
    }

    /**
     * maskSensitiveConfigFields 的逆操作：将 vault.json 中的敏感信息注入连接的 config。
     *
     * 注入优先级：
     * 1. password：直接查找 {connName}_password，按 schema 路径写入
     * 2. uri：查找 {connName}_uri
     *    - 若 schema 中存在 database_uri（如 MongoDB）→ 整体直接写入 uri 字段
     *    - 否则 → 按 "host:port/username" 格式解析，拆分写入 host/port/username
     * 3. url（仅当 vault 中无 uri 时）：查找 {connName}_url，同样按 "host:port/username" 解析写入
     */
    static void injectVaultSecretsToConnection(DataSourceConnectionDto conn,
            Map<String, String> vaultSecrets, DataSourceDefinitionDto definition) {
        if (conn == null || MapUtils.isEmpty(vaultSecrets)) {
            return;
        }
        String connectionName = conn.getName();
        if (StringUtils.isBlank(connectionName)) {
            log.warn("Vault inject skipped: connection name is blank");
            return;
        }
        Map<String, Object> config = conn.getConfig();
        if (config == null) {
            config = new LinkedHashMap<>();
            conn.setConfig(config);
        }
        final Map<String, Object> finalConfig = config;

        // 使用 BFS 工具方法获取 apiServerKey -> configPath 映射
        Map<String, String> apiKeyToConfigPath = new LinkedHashMap<>();
        if (definition != null) {
            Map<String, String> pathToApiKey = buildConfigPathToApiKeyMap(definition);
            if (pathToApiKey.isEmpty()) {
                log.warn("Vault inject: definition schema missing 'connection.properties' for connection '{}', pdkType={}",
                        connectionName, conn.getDatabase_type());
            }
            for (Map.Entry<String, String> entry : pathToApiKey.entrySet()) {
                if (VAULT_SUFFIX_TO_API_KEY.containsValue(entry.getValue())) {
                    apiKeyToConfigPath.put(entry.getValue(), entry.getKey());
                }
            }
        } else {
            log.warn("Vault inject: definition is null for connection '{}', pdkHash={}",
                    connectionName, conn.getPdkHash());
        }
        log.info("Vault inject: connection='{}', apiKeyToConfigPath={}", connectionName, apiKeyToConfigPath);

        boolean hasDatabaseUri = apiKeyToConfigPath.containsKey("database_uri");

        // 优先级1：查找 {connectionName}_uri
        String uriVaultKey = findVaultKey(vaultSecrets, connectionName, "uri");
        if (uriVaultKey != null) {
            String uriValue = vaultSecrets.get(uriVaultKey);
            if (hasDatabaseUri) {
                String configPath = apiKeyToConfigPath.get("database_uri");
                log.info("Vault inject: connection='{}', configPath='{}' <- uri (direct)", connectionName, configPath);
                setNestedValue(finalConfig, configPath, uriValue);
            } else {
                log.info("Vault inject: connection='{}', no database_uri in schema, parsing uri='{}'", connectionName, uriValue);
                injectFromUriString(finalConfig, uriValue, connectionName, "uri", apiKeyToConfigPath);
            }
            return;
        }

        // 优先级2：查找 {connectionName}_url + _user + _password
        String[] resolved = resolveVaultStrategy(vaultSecrets, connectionName);
        if (resolved != null) {
            log.info("Vault inject: connection='{}', resolved with prefix='{}'", connectionName, connectionName);
            applyResolvedVaultValues(finalConfig, vaultSecrets, resolved, connectionName, apiKeyToConfigPath, hasDatabaseUri);
            return;
        }

        // 优先级3：截取连接名后查找
        String truncated = truncateName(connectionName);
        if (truncated != null) {
            resolved = resolveVaultStrategy(vaultSecrets, truncated);
            if (resolved != null) {
                log.info("Vault inject: connection='{}', resolved with truncated prefix='{}'", connectionName, truncated);
                applyResolvedVaultValues(finalConfig, vaultSecrets, resolved, connectionName, apiKeyToConfigPath, hasDatabaseUri);
                return;
            }
        }

        // 优先级4：使用 default 前缀查找
        resolved = resolveVaultStrategy(vaultSecrets, "default");
        if (resolved != null) {
            log.info("Vault inject: connection='{}', resolved with prefix='default'", connectionName);
            applyResolvedVaultValues(finalConfig, vaultSecrets, resolved, connectionName, apiKeyToConfigPath, hasDatabaseUri);
            return;
        }

        // 优先级5：所有策略均未命中，报错退出
        throw new IllegalArgumentException(
                "Vault inject: connection='" + connectionName + "' has no matching vault keys (tried: "
                        + connectionName + ", " + (truncated != null ? truncated : "<no truncation>") + ", default)");
    }

    /**
     * 在 vaultSecrets 中查找 {prefix}_url、{prefix}_user、{prefix}_password 三个 key。
     * 三个都找到则返回 [urlKey, userKey, passwordKey]，否则返回 null。
     */
    private static String[] resolveVaultStrategy(Map<String, String> vaultSecrets, String prefix) {
        String urlKey = findVaultKey(vaultSecrets, prefix, "url");
        String userKey = findVaultKey(vaultSecrets, prefix, "user");
        String passwordKey = findVaultKey(vaultSecrets, prefix, "password");
        if (urlKey != null && userKey != null && passwordKey != null) {
            return new String[]{urlKey, userKey, passwordKey};
        }
        return null;
    }

    /**
     * 将 resolveVaultStrategy 找到的 url/user/password vault key 注入 config。
     */
    private static void applyResolvedVaultValues(Map<String, Object> finalConfig, Map<String, String> vaultSecrets,
            String[] resolvedKeys, String connectionName, Map<String, String> apiKeyToConfigPath, boolean hasDatabaseUri) {
        String urlValue = vaultSecrets.get(resolvedKeys[0]);
        String userValue = vaultSecrets.get(resolvedKeys[1]);
        String passwordValue = vaultSecrets.get(resolvedKeys[2]);

        // 注入 password
        String pwApiKey = VAULT_SUFFIX_TO_API_KEY.get("password");
        String pwConfigPath = apiKeyToConfigPath.get(pwApiKey);
        if (pwConfigPath == null) pwConfigPath = VAULT_SUFFIX_FALLBACK_CONFIG_KEY.get("password");
        if (pwConfigPath != null) {
            log.info("Vault inject: connection='{}', configPath='{}' <- password", connectionName, pwConfigPath);
            setNestedValue(finalConfig, pwConfigPath, passwordValue);
        }

        // 注入 user
        String userApiKey = VAULT_SUFFIX_TO_API_KEY.get("user");
        String userConfigPath = apiKeyToConfigPath.get(userApiKey);
        if (userConfigPath == null) userConfigPath = VAULT_SUFFIX_FALLBACK_CONFIG_KEY.get("user");
        if (userConfigPath != null) {
            log.info("Vault inject: connection='{}', configPath='{}' <- user", connectionName, userConfigPath);
            setNestedValue(finalConfig, userConfigPath, userValue);
        }

        // 注入 url → 解析为 host:port 写入（user 已单独注入，仅提取 host/port）
        if (hasDatabaseUri) {
            log.warn("Vault inject: connection='{}', has database_uri schema but resolved via url strategy", connectionName);
        }
        log.info("Vault inject: connection='{}', parsing url='{}' for host/port", connectionName, urlValue);
        Map<String, Object> components = parseUriComponents(urlValue);
        if (components.get("host") != null) {
            injectParsedField(finalConfig, connectionName, "host", components.get("host"), apiKeyToConfigPath);
        }
        if (components.get("port") != null) {
            injectParsedField(finalConfig, connectionName, "port", components.get("port"), apiKeyToConfigPath);
        }
    }

    /**
     * 注入单个字段（password 等），schema BFS 找不到时用 fallback config key。
     */
    private static void injectSingleField(Map<String, Object> config, Map<String, String> vaultSecrets,
            String connectionName, String vaultSuffix, Map<String, String> apiKeyToConfigPath) {
        String vaultKey = findVaultKey(vaultSecrets, connectionName, vaultSuffix);
        if (vaultKey == null) {
            log.debug("Vault inject: no vault key for connection='{}', suffix='{}'", connectionName, vaultSuffix);
            return;
        }
        String value = vaultSecrets.get(vaultKey);
        String apiKey = VAULT_SUFFIX_TO_API_KEY.get(vaultSuffix);
        String configPath = apiKeyToConfigPath.get(apiKey);
        if (configPath == null) {
            configPath = VAULT_SUFFIX_FALLBACK_CONFIG_KEY.get(vaultSuffix);
            log.warn("Vault inject: no schema configPath for apiKey='{}', using fallback='{}'", apiKey, configPath);
        }
        if (configPath == null) {
            log.warn("Vault inject: no configPath for suffix='{}', skipping", vaultSuffix);
            return;
        }
        log.info("Vault inject: connection='{}', configPath='{}' <- vaultKey='{}'", connectionName, configPath, vaultKey);
        setNestedValue(config, configPath, value);
    }

    /**
     * 将 URI/URL 字符串按 "host:port/username" 格式解析，分别写入 host、port、username 的 config 路径。
     * 若任意一项解析不到则抛出异常。
     */
    private static void injectFromUriString(Map<String, Object> config, String uriStr,
            String connectionName, String sourceLabel, Map<String, String> apiKeyToConfigPath) {
        Map<String, Object> components = parseUriComponents(uriStr);
        log.info("Vault inject ({}): connection='{}', parsed components={}", sourceLabel, connectionName, components);

        List<String> missing = new ArrayList<>();
        if (components.get("host") == null) missing.add("host");
        if (components.get("port") == null) missing.add("port");
        if (components.get("user") == null) missing.add("username");
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Vault inject: connection='" + connectionName + "', " + sourceLabel + "='" + uriStr
                            + "', missing components: " + missing);
        }

        injectParsedField(config, connectionName, "host", components.get("host"), apiKeyToConfigPath);
        injectParsedField(config, connectionName, "port", components.get("port"), apiKeyToConfigPath);
        injectParsedField(config, connectionName, "user", components.get("user"), apiKeyToConfigPath);
    }

    private static void injectParsedField(Map<String, Object> config, String connectionName,
            String vaultSuffix, Object value, Map<String, String> apiKeyToConfigPath) {
        if (value == null) return;
        String apiKey = VAULT_SUFFIX_TO_API_KEY.get(vaultSuffix);
        String configPath = apiKeyToConfigPath.get(apiKey);
        if (configPath == null) configPath = VAULT_SUFFIX_FALLBACK_CONFIG_KEY.get(vaultSuffix);
        if (configPath == null) {
            log.warn("Vault inject: no configPath for suffix='{}', skipping", vaultSuffix);
            return;
        }
        log.info("Vault inject: connection='{}', configPath='{}' <- {} (parsed)", connectionName, configPath, value);
        setNestedValue(config, configPath, value);
    }

    /**
     * 解析 URI 字符串，提取 host、port、user（username）。
     * 支持标准格式：scheme://user:password@host:port/db
     * 支持简化格式：host:port/username（无 scheme、无密码）
     */
    private static Map<String, Object> parseUriComponents(String uriStr) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (StringUtils.isBlank(uriStr)) return result;
        try {
            // 无 scheme 时补一个虚拟 scheme，使 java.net.URI 能正常解析
            String toParse = uriStr.contains("://") ? uriStr : "dummy://" + uriStr;
            java.net.URI uri = new java.net.URI(toParse);

            String host = uri.getHost();
            int    port = uri.getPort();
            String userInfo = uri.getUserInfo();
            String path     = uri.getPath();

            if (StringUtils.isNotBlank(host)) result.put("host", host);
            if (port > 0)                      result.put("port", port);

            // username 优先从 userInfo 取（user:password@host 格式），
            // 其次从 path 首段取（host:port/username 格式，如 localhost:8080/user）
            if (StringUtils.isNotBlank(userInfo)) {
                int colonIdx = userInfo.indexOf(':');
                String username = colonIdx > 0 ? userInfo.substring(0, colonIdx) : userInfo;
                if (StringUtils.isNotBlank(username)) result.put("user", username);
            } else if (StringUtils.isNotBlank(path) && path.length() > 1) {
                String segment = path.startsWith("/") ? path.substring(1) : path;
                int slashIdx = segment.indexOf('/');
                if (slashIdx > 0) segment = segment.substring(0, slashIdx);
                if (StringUtils.isNotBlank(segment)) result.put("user", segment);
            }
        } catch (Exception e) {
            log.warn("Vault inject: failed to parse URI components from '{}': {}", uriStr, e.getMessage());
        }
        return result;
    }

    /**
     * 在 vaultSecrets 中精确查找 key = "{prefix}_{suffix}"（忽略大小写）。
     */
    private static String findVaultKey(Map<String, String> vaultSecrets, String prefix, String suffix) {
        String target = (prefix + "_" + suffix).toLowerCase(Locale.ROOT);
        for (String key : vaultSecrets.keySet()) {
            if (key.toLowerCase(Locale.ROOT).equals(target)) {
                return key;
            }
        }
        return null;
    }

    /**
     * 截取连接名第二个下划线之前的部分。
     * 例如：TMH_PG_HPI → TMH_PG；ABC_DEF → null（只有一个下划线）；ABCDEF → null（无下划线）
     */
    static String truncateName(String name) {
        if (name == null) return null;
        int first = name.indexOf('_');
        if (first < 0) return null;
        int second = name.indexOf('_', first + 1);
        if (second < 0) return null;
        return name.substring(0, second);
    }

    /**
     * 从 config map 中按点号分隔的路径读取值（支持嵌套路径，如 "ssl.password"）
     */
    static Object getNestedValue(Map<String, Object> config, String path) {
        if (config == null || path == null) return null;
        String[] parts = path.split("\\.");
        Object current = config;
        for (String part : parts) {
            if (!(current instanceof Map)) return null;
            current = ((Map<String, Object>) current).get(part);
        }
        return current;
    }

    /**
     * 在 config map 中按点号分隔的路径写入值（支持嵌套路径，如 "ssl.password"）
     */
    private static void setNestedValue(Map<String, Object> config, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = config;
        for (int i = 0; i < parts.length - 1; i++) {
            current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new LinkedHashMap<>());
        }
        current.put(parts[parts.length - 1], value);
    }

    /**
     * 从 config map 中按点号分隔的路径删除对应值（支持嵌套路径，如 "ssl.password"）
     */
    static void removeNestedValue(Map<String, Object> config, String path) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = config;
        for (int i = 0; i < parts.length - 1; i++) {
            Object next = current.get(parts[i]);
            if (!(next instanceof Map)) {
                return;
            }
            current = (Map<String, Object>) next;
        }
        current.remove(parts[parts.length - 1]);
    }

}
