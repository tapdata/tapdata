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
            "database_password"
    );

    /**
     * vault.json 字段后缀 -> 标准化 apiServerKey 的映射
     * 对应 vault key 格式：{connectionName}_{suffix}
     */
    Map<String, String> VAULT_SUFFIX_TO_API_KEY = Map.of(
            "host",     "database_host",
            "port",     "database_port",
            "user",     "database_username",
            "password", "database_password"
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
    List<DataSourceConnectionDto> loadConnections(List<?> resources);

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
        List<DataSourceConnectionDto> connections = loadConnections(resources);
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
    default List<TaskUpAndLoadDto> buildConnectionPayload(List<DataSourceConnectionDto> connections, UserDetail user) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        if (CollectionUtils.isEmpty(connections)) {
            return payload;
        }

        for (DataSourceConnectionDto dataSourceConnectionDto : connections) {
            dataSourceConnectionDto.setConnectionString(null);
            dataSourceConnectionDto.setCreateUser(null);
            dataSourceConnectionDto.setCustomId(null);
            dataSourceConnectionDto.setLastUpdBy(null);
            dataSourceConnectionDto.setUserId(null);
            // 移除环境相关字段，避免跨环境导入时产生误差
            if (dataSourceConnectionDto.getConfig() != null) {
                dataSourceConnectionDto.getConfig().remove("datasourceInstanceId");
            }
            DataSourceDefinitionService dataSourceDefinitionService = SpringUtil
                    .getBean(DataSourceDefinitionService.class);
            DataSourceDefinitionDto definition = dataSourceDefinitionService
                    .findByPdkHash(dataSourceConnectionDto.getPdkHash(), Integer.MAX_VALUE, user);
            if (definition != null) {
                dataSourceConnectionDto.setDefinitionPdkAPIVersion(definition.getPdkAPIVersion());
                maskSensitiveConfigFields(dataSourceConnectionDto, definition);
            }

            // 收集元数据
            String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database",
                    dataSourceConnectionDto, null);
            MetadataInstancesService metadataInstancesService = SpringUtil.getBean(MetadataInstancesService.class);
            MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
                    Query.query(Criteria.where("qualified_name").is(databaseQualifiedName).and("is_deleted").ne(true)),
                    user);
            if (dataSourceMetadataInstance != null) {
                payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_METADATA_INSTANCES,
                        JsonUtil.toJsonUseJackson(dataSourceMetadataInstance)));
            }

            // 存储连接数据（用于后续统一生成Excel）
            payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_CONNECTION,
                    JsonUtil.toJsonUseJackson(dataSourceConnectionDto)));
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
                        if (connectionDto != null) {
                            String key = connectionDto.getId() == null ? connectionDto.getName()
                                    : connectionDto.getId().toHexString();
                            connections.putIfAbsent(key, connectionDto);
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
                    String key = connectionDto.getId() == null ? connectionDto.getName()
                            : connectionDto.getId().toHexString();
                    connections.putIfAbsent(key, connectionDto);
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
                    String key = metadataDefinitionDto.getId() == null ? metadataDefinitionDto.getValue()
                            : metadataDefinitionDto.getId().toHexString();
                    metadataDefinitions.putIfAbsent(key, metadataDefinitionDto);
                }
            }

        }
    }

    /**
     * 根据 DataSourceDefinitionDto 中的 apiServerKey 定义，找到 DataSourceConnectionDto.config
     * 里对应的路径并清空值。只处理 SENSITIVE_API_KEYS 中声明的标准化 apiServerKey。
     */
    private static void maskSensitiveConfigFields(DataSourceConnectionDto conn,
            DataSourceDefinitionDto definition) {
        Map<String, Object> config = conn.getConfig();
        if (MapUtils.isEmpty(config) || definition == null) {
            return;
        }
        LinkedHashMap<String, Object> properties = definition.getProperties();
        if (properties == null) {
            return;
        }
        Object connection = properties.get("connection");
        if (!(connection instanceof Map)) {
            return;
        }
        Object connectionProperties = ((Map<?, ?>) connection).get("properties");
        if (!(connectionProperties instanceof Map)) {
            return;
        }
        // BFS 遍历 connection.properties，找到 apiServerKey 命中敏感集合的字段路径
        // 字段路径（如 "host"、"ssl.password"）即是 config 中的 key，直接删除对应值
        Deque<Object[]> queue = new ArrayDeque<>();
        queue.add(new Object[]{connectionProperties, ""});
        while (!queue.isEmpty()) {
            Object[] node = queue.poll();
            Map<String, Object> props = (Map<String, Object>) node[0];
            String prefix = (String) node[1];
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                if (!(entry.getValue() instanceof Map)) {
                    continue;
                }
                Map<String, Object> meta = (Map<String, Object>) entry.getValue();
                String configPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                Object apiServerKey = meta.get("apiServerKey");
                if (apiServerKey instanceof String && SENSITIVE_API_KEYS.contains(apiServerKey)) {
                    removeNestedValue(config, configPath);
                }
                Object childProperties = meta.get("properties");
                if (childProperties instanceof Map) {
                    queue.add(new Object[]{childProperties, configPath});
                }
            }
        }
    }

    /**
     * maskSensitiveConfigFields 的逆操作：将 vault.json 中的敏感信息注入连接的 config。
     * BFS 遍历 definition schema，找到各敏感 apiServerKey 对应的 config 路径，
     * 按 "{connectionName}_{vaultSuffix}" 格式查找 vault 值并写入。
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

        // 通过 definition schema BFS 找到 apiServerKey 对应的 config 路径
        Map<String, String> apiKeyToConfigPath = new LinkedHashMap<>();
        if (definition != null) {
            LinkedHashMap<String, Object> properties = definition.getProperties();
            Object connection = properties != null ? properties.get("connection") : null;
            Object connectionProperties = (connection instanceof Map)
                    ? ((Map<?, ?>) connection).get("properties") : null;
            if (connectionProperties instanceof Map) {
                Deque<Object[]> queue = new ArrayDeque<>();
                queue.add(new Object[]{connectionProperties, ""});
                while (!queue.isEmpty()) {
                    Object[] node = queue.poll();
                    Map<String, Object> props = (Map<String, Object>) node[0];
                    String prefix = (String) node[1];
                    for (Map.Entry<String, Object> entry : props.entrySet()) {
                        if (!(entry.getValue() instanceof Map)) {
                            continue;
                        }
                        Map<String, Object> meta = (Map<String, Object>) entry.getValue();
                        String configPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                        Object apiServerKey = meta.get("apiServerKey");
                        if (apiServerKey instanceof String && VAULT_SUFFIX_TO_API_KEY.containsValue(apiServerKey)) {
                            apiKeyToConfigPath.put((String) apiServerKey, configPath);
                        }
                        Object childProperties = meta.get("properties");
                        if (childProperties instanceof Map) {
                            queue.add(new Object[]{childProperties, configPath});
                        }
                    }
                }
            } else {
                log.warn("Vault inject: definition schema missing 'connection.properties' for connection '{}', pdkType={}",
                        connectionName, conn.getDatabase_type());
            }
        } else {
            log.warn("Vault inject: definition is null for connection '{}', pdkHash={}",
                    connectionName, conn.getPdkHash());
        }

        log.info("Vault inject: connection='{}', apiKeyToConfigPath={}", connectionName, apiKeyToConfigPath);

        // 按 vault key 格式 "{connectionName}_{suffix}" 查找并注入
        // 若 schema BFS 找不到 configPath，退而使用 fallback 默认 config key
        VAULT_SUFFIX_TO_API_KEY.forEach((vaultSuffix, apiKey) -> {
            // 先精确匹配，再扫描 vault 找前缀匹配连接名的 key（兼容连接名含空格等差异）
            String vaultKey = findVaultKey(vaultSecrets, connectionName, vaultSuffix);
            if (vaultKey == null) {
                log.debug("Vault inject: no vault key for connection='{}', suffix='{}'", connectionName, vaultSuffix);
                return;
            }
            String value = vaultSecrets.get(vaultKey);
            String configPath = apiKeyToConfigPath.get(apiKey);
            if (configPath == null) {
                configPath = VAULT_SUFFIX_FALLBACK_CONFIG_KEY.get(vaultSuffix);
                log.warn("Vault inject: no schema configPath for apiKey='{}', using fallback configPath='{}' (vaultKey='{}')",
                        apiKey, configPath, vaultKey);
            }
            if (configPath == null) {
                log.warn("Vault inject: no configPath (schema or fallback) for apiKey='{}' (vaultKey='{}'), skipping", apiKey, vaultKey);
                return;
            }
            // port 字段需写入数字类型
            Object configValue = "port".equals(vaultSuffix) ? Integer.parseInt(value) : value;
            log.info("Vault inject: connection='{}', configPath='{}' <- vaultKey='{}', value={}", connectionName, configPath, vaultKey, configValue);
            setNestedValue(finalConfig, configPath, configValue);
        });
    }

    /**
     * 在 vaultSecrets 中查找与连接名和字段匹配的 vault key。
     * vault key 格式：{team}_{hospital}_{db_type}_{connectionName}_{field}
     * 匹配条件：key 以 "_{connectionName}_{suffix}" 结尾
     */
    private static String findVaultKey(Map<String, String> vaultSecrets, String connectionName, String suffix) {
        String tail = "_" + connectionName + "_" + suffix;
        for (String key : vaultSecrets.keySet()) {
            if (key.endsWith(tail)) {
                return key;
            }
        }
        return null;
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
    private static void removeNestedValue(Map<String, Object> config, String path) {
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
