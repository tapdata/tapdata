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
import com.tapdata.tm.commons.schema.bean.SourceDto;
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
            "uri",      "database_uri",
            // 格式 3 新增：库名不是 vault 里的独立键，而是从 DSN 解析出来的成分，
            // 但它要走同一套 apiServerKey -> configPath 查表，故占一个伪 suffix。
            // ⚠ 它**不进** SENSITIVE_API_KEYS——库名不是凭据，导出不脱敏（[ADR-0036]）。
            "database", "database_name"
    );

    /**
     * 当 schema BFS 无法找到 apiServerKey 映射时，使用 fallback 默认 config key。
     * vault suffix -> 默认 config key（直接写入 config 根级别）
     */
    Map<String, String> VAULT_SUFFIX_FALLBACK_CONFIG_KEY = Map.of(
            "host",     "host",
            "port",     "port",
            "user",     "username",
            "password", "password",
            // schema BFS 落空时库名的落点。选 "database" 而非 "databaseName"/"database_name"：
            // 与同表其余四项一样取 PDK config 的通用根键名（[ADR-0036] D8 要求把它定死，
            // 否则 schema 缺失的连接会静默丢库名）。
            "database", "database"
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
     * @param resources   资源列表
     * @param user        用户信息
     * @param maskSecrets 是否移除包内凭据（[ADR-0034] D1/D2）。任务导出会把 DAG 各节点的
     *                    {@link MetadataInstancesDto} 一并塞进包，其 {@code source} 是连接的
     *                    整份拷贝、带顶层凭据镜像 —— 这是继连接文档、database 级与 table 级
     *                    metadata 之后的**第四处载体**，2026-08-06 实机在真实包里抓到
     *                    （老包同样在漏，故非回归）。
     *                    <p>刻意不留带默认值的重载：一个能被静默取到的默认，就是下一次把明文
     *                    凭据推上 GitHub 的入口。
     * @return 导出数据 payload 列表
     */
    List<TaskUpAndLoadDto> buildExportPayload(List<?> resources, UserDetail user, boolean maskSecrets);

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

    /**
     * @param maskSecrets 是否抹掉连接凭据。**刻意不提供带默认值的重载**：调用方必须自己声明意图——
     *                    一个能被静默取到的默认，就是将来把明文凭据推上 GitHub 的入口（[ADR-0034] D2）
     */
    default void handleRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloadsByType, List<?> resources,
            UserDetail user,Set<ObjectId> tagIds, boolean maskSecrets) {
        List<DataSourceEntity> connections = loadConnections(resources);
        List<TaskUpAndLoadDto> connectionPayload = buildConnectionPayload(connections, user, maskSecrets);
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
     *
     * @param maskSecrets 是否抹掉 config 里的敏感字段。FILE 默认保真、GIT 强制脱敏，
     *                    口径由 {@code GroupInfoService.resolveMaskSecrets} 单点解析（[ADR-0034] D1/D2）
     */
    default List<TaskUpAndLoadDto> buildConnectionPayload(List<DataSourceEntity> connections, UserDetail user,
            boolean maskSecrets) {
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
            if (maskSecrets) {
                if (definition != null) {
                    maskSensitiveConfigFields(entity, definition);
                }
                // config 之外还有一份顶层镜像，不抹等于没抹（[ADR-0034] D2）
                maskMirroredSecretFields(entity);
            }

            // 收集元数据
            String id = entity.getId().toHexString();
            String databaseQualifiedName;
            if (DataSourceDefinitionDto.PDK_TYPE.equals(entity.getPdkType())) {
                databaseQualifiedName = MetaDataBuilderUtils.generatePdkQualifiedName(
                        "database", id, "", entity.getDefinitionPdkId(),
                        entity.getDefinitionGroup(), entity.getDefinitionVersion(), null);
            } else {
                databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", id, null);
            }
            MetadataInstancesService metadataInstancesService = SpringUtil.getBean(MetadataInstancesService.class);
            MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
                    Query.query(Criteria.where("qualified_name").is(databaseQualifiedName).and("is_deleted").ne(true)),
                    user);
            if (dataSourceMetadataInstance != null) {
                if (maskSecrets) {
                    // metadata 的 source 是连接的整份拷贝（DAGService 序列化而来），凭据的第三处存放点
                    maskMirroredSecretFields(dataSourceMetadataInstance);
                }
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
    /**
     * 顶层凭据镜像字段：{@link #SENSITIVE_API_KEYS} 那一批 secret 在 {@code config} **之外**的第二处存放点。
     *
     * 连接实体、以及 {@code MetadataInstances.source}（连接的整份拷贝）都各有一份，字段名完全相同。
     * 这不是「扩敏感字段集合」（[ADR-0034] 明确不改 {@code SENSITIVE_API_KEYS} 的成员），
     * 而是把**既有的那一组 secret** 应用到之前被漏掉的载体上 —— 只抹 config 的话，
     * 导出包里照样躺着明文 uri/password。
     *
     * {@code plain_password} / {@code database_password_1} 是同一个口令的别名；
     * {@code database_name} / {@code database_type} 等不是凭据，不动。证书材料（{@code sslPass} 等）
     * 属 ADR 划出的「扩敏感字段集合是另一件事」，不在此列。
     */
    List<String> MIRRORED_SECRET_FIELDS = List.of(
            "database_host", "database_username", "database_port",
            "database_uri", "database_password", "plain_password", "database_password_1");

    /** 抹掉连接实体上的顶层凭据镜像（导出侧）。 */
    static void maskMirroredSecretFields(DataSourceEntity conn) {
        if (conn == null) {
            return;
        }
        conn.setDatabase_host(null);
        conn.setDatabase_username(null);
        conn.setDatabase_port(null);
        conn.setDatabase_uri(null);
        conn.setDatabase_password(null);
        conn.setPlain_password(null);
        conn.setDatabase_password_1(null);
    }

    /** 抹掉 metadata 里那份连接拷贝上的顶层凭据镜像（导出侧）。 */
    static void maskMirroredSecretFields(MetadataInstancesDto metadata) {
        if (metadata == null || metadata.getSource() == null) {
            return;
        }
        SourceDto source = metadata.getSource();
        source.setDatabase_host(null);
        source.setDatabase_username(null);
        source.setDatabase_port(null);
        source.setDatabase_uri(null);
        source.setDatabase_password(null);
        source.setPlain_password(null);
        source.setDatabase_password_1(null);
    }

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
     * <p><b>注入优先级——六级，逐级下降，命中即停。</b> 这条链在三处各有一份表述
     * （本注释、worker 的 {@code generate-vault.sh} 里两处），三处必须对得上：
     * 漂开一处就够让人把 GitHub 那边配错，而配错的表现是「部署报绿、连接连的是别的东西」。
     *
     * <ol>
     *   <li><b>dsn</b>（格式 3）：{@code {conn}_DSN}（Variables）+ {@code {conn}_PASSWORD}（Secrets）。
     *       schema 含 {@code database_uri}（MongoDB）→ 密码 splice 回 userinfo 后**整串直写**；
     *       否则（JDBC）→ 归一化后拆成 host/port/username/<b>database_name</b>。
     *       <b>只认精确连接名</b>，不回落（回落＝连错库＝数据事故，[ADR-0036] D4）。</li>
     *   <li><b>uri</b>（格式 1）：{@code {conn}_URI}。schema 含 {@code database_uri} → 整串直写；
     *       否则按 {@code host:port/username} 解析。<b>此路径从不注入 password。</b></li>
     *   <li><b>url + user + password</b>（格式 2）：三个键用<b>精确</b>连接名，缺一不可。</li>
     *   <li>同上三键，用<b>截断</b>后的连接名（{@code A_B_C} → {@code A_B}）。</li>
     *   <li>同上三键，用 {@code default} 前缀。</li>
     *   <li>全不命中 → 抛 {@link IllegalArgumentException}。⚠ 调用方
     *       {@code GroupInfoService.injectVaultSecrets} <b>没有 per-connection try/catch</b>，
     *       所以这一抛会中止<b>整批</b>连接导入——文案因此要点名 DSN（见 [ADR-0036] D12 的升级顺序）。</li>
     * </ol>
     *
     * <p>缺值一律走同一条规则（[ADR-0036] D10）：<b>不报错、不写空值</b>，保留目标环境既有值，
     * 并发一条逐字点名缺了什么的 WARN。写空值会让 [ADR-0034] D5 误以为「包里有值」而放行，
     * 目标凭据当场被抹空。
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

        // 优先级1：格式 3——{connectionName}_DSN（Variables）+ {connectionName}_PASSWORD（Secrets）
        String dsnVaultKey = findVaultKey(vaultSecrets, connectionName, "dsn");
        if (dsnVaultKey != null) {
            String dsnValue = vaultSecrets.get(dsnVaultKey);
            String pwVaultKey = findVaultKey(vaultSecrets, connectionName, "password");
            String pwValue = pwVaultKey == null ? null : vaultSecrets.get(pwVaultKey);
            if (pwValue == null) {
                // 缺值统一规则（[ADR-0036] D10）：不报错、不写空值，但要**逐字点名那个键**——
                // 泛化的告警读不出该去配哪个键，而拼错键名与「真的没密码」在数据上完全一样。
                log.warn("Vault inject: connection='{}', no {}_PASSWORD in vault; keeping the target's"
                        + " existing password. If this connection does have a password, that key name"
                        + " is misspelled or missing.", connectionName, connectionName);
            }
            if (hasDatabaseUri) {
                String configPath = apiKeyToConfigPath.get("database_uri");
                String uriWithPassword = splicePasswordIntoDsn(dsnValue, pwValue);
                validateMongoDsn(uriWithPassword, connectionName, pwValue);
                log.info("Vault inject: connection='{}', configPath='{}' <- dsn (direct)", connectionName, configPath);
                setNestedValue(finalConfig, configPath, uriWithPassword);
                // 顶层镜像字段与 config 一起写（[ADR-0036] D9）。保存链路**不会**从 config
                // 重推顶层，而 MetaDataBuilderUtils.generateQualifiedName 建元数据限定名读的
                // 正是顶层——只写 config 会得到「连接连新库、元数据挂旧库名」的半改状态。
                // 必须写在 preserveExistingSecrets 之前：restoreMirroredField 只在 incoming
                // 缺该字段时回填，不写就会被目标环境的旧值填上。
                conn.setDatabase_uri(uriWithPassword);
            } else {
                Map<String, Object> parts = parseDsnComponents(dsnValue);
                log.info("Vault inject (dsn): connection='{}', parsed components={}", connectionName, parts);
                if (parts.get("query") != null) {
                    // [ADR-0036] D11：JDBC 的 query 串本期不解析、丢弃并告警。丢弃必须是响亮的——
                    // 静默丢弃会让「参数配了但没生效」在目标环境才浮现。参数仍走包内 additionalString。
                    log.warn("Vault inject: connection='{}', the DSN query string '?{}' is discarded"
                            + " (not supported in this release); connection parameters still come from"
                            + " the package's additionalString.", connectionName, parts.get("query"));
                }
                if (parts.get("user") == null) {
                    log.warn("Vault inject: connection='{}', the DSN carries no userinfo, so no username"
                            + " was taken from it; keeping the target's existing username."
                            + " Format 3 does not read {}_USER.", connectionName, connectionName);
                }
                if (parts.get("database") == null) {
                    log.warn("Vault inject: connection='{}', the DSN carries no database name;"
                            + " keeping the target's existing database name.", connectionName);
                }
                injectParsedField(finalConfig, connectionName, "host", parts.get("host"), apiKeyToConfigPath);
                injectParsedField(finalConfig, connectionName, "port", parts.get("port"), apiKeyToConfigPath);
                injectParsedField(finalConfig, connectionName, "user", parts.get("user"), apiKeyToConfigPath);
                injectParsedField(finalConfig, connectionName, "database", parts.get("database"), apiKeyToConfigPath);
                if (pwValue != null) {
                    injectParsedField(finalConfig, connectionName, "password", pwValue, apiKeyToConfigPath);
                }
                // 顶层镜像字段，同上（[ADR-0036] D9）。⚠ 顶层 database_password 刻意不写：
                // 它存的是 AES 密文（beforeSave 由 plain_password 推出），塞明文会毁掉那个契约；
                // 密码的缺值语义由 [ADR-0034] D5 管，连接器读的是 config 那一份。
                if (parts.get("host") != null)     conn.setDatabase_host((String) parts.get("host"));
                if (parts.get("port") != null)     conn.setDatabase_port((Integer) parts.get("port"));
                if (parts.get("user") != null)     conn.setDatabase_username((String) parts.get("user"));
                if (parts.get("database") != null) conn.setDatabase_name((String) parts.get("database"));
            }
            return;
        }

        // 优先级2：查找 {connectionName}_uri
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

        // 优先级3：查找 {connectionName}_url + _user + _password
        String[] resolved = resolveVaultStrategy(vaultSecrets, connectionName);
        if (resolved != null) {
            log.info("Vault inject: connection='{}', resolved with prefix='{}'", connectionName, connectionName);
            applyResolvedVaultValues(finalConfig, vaultSecrets, resolved, connectionName, apiKeyToConfigPath, hasDatabaseUri);
            return;
        }

        // 优先级4：截取连接名后查找
        String truncated = truncateName(connectionName);
        if (truncated != null) {
            resolved = resolveVaultStrategy(vaultSecrets, truncated);
            if (resolved != null) {
                log.info("Vault inject: connection='{}', resolved with truncated prefix='{}'", connectionName, truncated);
                applyResolvedVaultValues(finalConfig, vaultSecrets, resolved, connectionName, apiKeyToConfigPath, hasDatabaseUri);
                return;
            }
        }

        // 优先级5：使用 default 前缀查找
        resolved = resolveVaultStrategy(vaultSecrets, "default");
        if (resolved != null) {
            log.info("Vault inject: connection='{}', resolved with prefix='default'", connectionName);
            applyResolvedVaultValues(finalConfig, vaultSecrets, resolved, connectionName, apiKeyToConfigPath, hasDatabaseUri);
            return;
        }

        // 优先级6：所有策略均未命中，报错退出
        // 文案点名 DSN 是刻意的（[ADR-0036] D12）：只含 _DSN 的 vault 撞上一个不认识格式 3 的 TM 时，
        // 走的正是这一条，而 injectVaultSecrets 没有 per-connection try/catch ⇒ 整批导入中止。
        // 不提 DSN 的话，那条报错读起来像租户把键配错了，而不是「这个环境的 TM 该升级了」。
        throw new IllegalArgumentException(
                "Vault inject: connection='" + connectionName + "' has no matching vault keys (tried: "
                        + connectionName + "_DSN, " + connectionName + "_URI, "
                        + connectionName + "_URL/_USER/_PASSWORD, "
                        + (truncated != null ? truncated : "<no truncation>") + ", default). "
                        + "If the repository is configured with " + connectionName
                        + "_DSN, this TM predates format 3 and needs upgrading first.");
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
    /**
     * 把 {@code {CONN}_PASSWORD} 补进 DSN 的 userinfo。
     *
     * 顺序是**先拼后验**（[ADR-0036] D8）：{@code ConnectionString} 会拒掉
     * {@code mongodb://u@h/db}（无冒号那种空密码写法），所以不能先解析再拼——
     * 那种形态在解析阶段就死了。改为在**原始串**上做字符串手术，再交给
     * {@code ConnectionString} 做最终校验。
     */
    private static String splicePasswordIntoDsn(String dsn, String password) {
        if (StringUtils.isBlank(dsn) || password == null) {
            return dsn;
        }
        int schemeIdx = dsn.indexOf("://");
        int userInfoStart = schemeIdx < 0 ? 0 : schemeIdx + 3;
        int atIdx = dsn.indexOf('@', userInfoStart);
        if (atIdx < 0) {
            // 无 userinfo：不 splice，交给缺值规则（[ADR-0036] D10）
            return dsn;
        }
        String userInfo = dsn.substring(userInfoStart, atIdx);
        int colonIdx = userInfo.indexOf(':');
        // 用户名**原样透传**：它是 DSN 作者写进 URI 的，已经该是编码态；再编码一次会把
        // `%40` 变成 `%2540`。作者漏编码时由末尾的 ConnectionString 校验抓出来，
        // 而不是我们悄悄替他改写（[ADR-0036] D8）。密码来自 Secrets、是裸值，必须由我们编码。
        String user = colonIdx < 0 ? userInfo : userInfo.substring(0, colonIdx);
        return dsn.substring(0, userInfoStart) + user + ":" + percentEncodeUserInfo(password)
                + dsn.substring(atIdx);
    }

    /**
     * 「先拼后验」的**验**（[ADR-0036] D8）：splice 完的串必须仍是合法 MongoDB 连接串。
     *
     * ⚠ 报错消息**绝不回显这个串**——splice 之后它带着明文密码。驱动本身的消息
     * 经实测不回显（2026-08-18 探针），但仍防御性地把密码从中抹掉，避免驱动升级后走样。
     */
    private static void validateMongoDsn(String splicedUri, String connectionName, String password) {
        if (StringUtils.isBlank(splicedUri)) return;
        try {
            new com.mongodb.ConnectionString(splicedUri);
        } catch (RuntimeException e) {
            String detail = e.getMessage() == null ? "" : e.getMessage();
            if (password != null && !password.isEmpty()) {
                detail = detail.replace(password, "***");
            }
            throw new IllegalArgumentException(
                    "Vault inject: connection='" + connectionName + "', the DSN is not a valid MongoDB"
                            + " connection string after splicing the password in: " + detail
                            + " (the DSN is not echoed here because it now carries the password;"
                            + " check that the username is percent-encoded)");
        }
    }

    /**
     * 按 RFC 3986 对 userinfo 成分做 percent-encoding：只放行 unreserved
     * （{@code ALPHA / DIGIT / - . _ ~}），其余一律编码。
     *
     * 刻意**不用** {@code URLEncoder}——那是 form 编码，会把空格写成 {@code +}，
     * 而 {@code +} 在 URI 的 userinfo 里是字面加号，密码含空格时就会反解错。
     */
    private static String percentEncodeUserInfo(String raw) {
        StringBuilder sb = new StringBuilder(raw.length() + 8);
        for (byte b : raw.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '.' || c == '_' || c == '~') {
                sb.append((char) c);
            } else {
                sb.append('%').append(String.format("%02X", c));
            }
        }
        return sb.toString();
    }

    /**
     * 解析格式 3 的 DSN，得到归一化目标四元组 {@code (host, port, user, database)}。
     *
     * ⚠ **刻意不复用 {@link #parseUriComponents}**（[ADR-0036] D5/D8）：那个函数在无 userinfo 时
     * 把 path 首段当 username，而 DSN 的 path 首段是**库名**（`localhost:3306/test` 会得到
     * `user=test`）；它底下的 {@code java.net.URI} 又把 `jdbc:xxx://…` 当 opaque URI，
     * host/port/path 全为 null ⇒ 整个 JDBC 分支静默空跑。
     *
     * 归一化必须发生在解析**之前**：先剥可选的 {@code jdbc:}，再剥可选的 {@code scheme://}。
     * 两个前缀都**只是入口宽容度、一律丢弃**，永不用于判型（类型来自连接自身的 definition/pdkHash）。
     */
    private static Map<String, Object> parseDsnComponents(String dsn) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (StringUtils.isBlank(dsn)) return result;

        String rest = dsn.trim();
        // 剥 scheme 这一步**同时**吃掉 `jdbc:` 前缀：D5 收的三种形态里带前缀的两种
        // （`jdbc:mysql://…` / `mysql://…`）都含 `://`，从它往后截即可。
        // ⚠ 刻意**没有**单独再剥一次 `jdbc:`：那一步对本 ADR 收的任何形态都不可达
        // （变异测试实证——去掉它没有任何用例变红），而对不含 `://` 的
        // `jdbc:mysql:user@h/db` 它反而解析出 user=`mysql`，是错的而不是更宽容的。
        int schemeIdx = rest.indexOf("://");
        if (schemeIdx >= 0) {
            rest = rest.substring(schemeIdx + 3);
        }

        int queryIdx = rest.indexOf('?');
        if (queryIdx >= 0) {
            result.put("query", rest.substring(queryIdx + 1));
            rest = rest.substring(0, queryIdx);
        }

        int atIdx = rest.lastIndexOf('@');
        if (atIdx >= 0) {
            String userInfo = rest.substring(0, atIdx);
            int colonIdx = userInfo.indexOf(':');
            String user = colonIdx < 0 ? userInfo : userInfo.substring(0, colonIdx);
            if (StringUtils.isNotBlank(user)) result.put("user", user);
            rest = rest.substring(atIdx + 1);
        }

        int slashIdx = rest.indexOf('/');
        if (slashIdx >= 0) {
            String database = rest.substring(slashIdx + 1);
            if (StringUtils.isNotBlank(database)) result.put("database", database);
            rest = rest.substring(0, slashIdx);
        }

        int colonIdx = rest.lastIndexOf(':');
        if (colonIdx >= 0) {
            String portStr = rest.substring(colonIdx + 1);
            if (StringUtils.isNotBlank(portStr) && portStr.chars().allMatch(Character::isDigit)) {
                result.put("port", Integer.parseInt(portStr));
                rest = rest.substring(0, colonIdx);
            }
        }
        if (StringUtils.isNotBlank(rest)) result.put("host", rest);
        return result;
    }

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
    /**
     * 导入侧：包内敏感字段缺失/为空时，保留目标环境已有的值（[ADR-0034] D5/D6）。
     *
     * 为什么需要它：导出会把敏感字段抹空，而 GROUP_IMPORT 的落库是整文档覆盖
     * （{@code DataSourceServiceImpl.handleGroupImportConnection} → {@code importSave}），
     * 于是「脱敏包 + 未提供 vault」会把目标环境已有的 uri/password 覆盖成空，导入还报成功。
     * 包里那个空缺是脱敏流程的产物、不是用户配置的内容，因此**任何 importMode 下都不该覆盖**。
     *
     * @return 被保留（即包内缺值、改用目标既有值）的 config path，供调用方汇报——D7 要求绝不静默。
     */
    static List<String> restoreMissingSecretsFromExisting(DataSourceConnectionDto incoming,
            DataSourceConnectionDto existing, DataSourceDefinitionDto definition) {
        List<String> preserved = new ArrayList<>();
        if (incoming == null || existing == null) {
            return preserved;
        }
        // 顶层镜像与 config 同等对待：导出把两处一起抹了，导入就得把两处一起补回来，
        // 否则 ES-2b 只是把「凭据被抹空」从 config 挪到了顶层
        restoreMirroredField("database_host", incoming.getDatabase_host(), existing.getDatabase_host(),
                incoming::setDatabase_host, preserved);
        restoreMirroredField("database_username", incoming.getDatabase_username(), existing.getDatabase_username(),
                incoming::setDatabase_username, preserved);
        restoreMirroredField("database_port", incoming.getDatabase_port(), existing.getDatabase_port(),
                incoming::setDatabase_port, preserved);
        restoreMirroredField("database_uri", incoming.getDatabase_uri(), existing.getDatabase_uri(),
                incoming::setDatabase_uri, preserved);
        restoreMirroredField("database_password", incoming.getDatabase_password(), existing.getDatabase_password(),
                incoming::setDatabase_password, preserved);
        restoreMirroredField("plain_password", incoming.getPlain_password(), existing.getPlain_password(),
                incoming::setPlain_password, preserved);
        restoreMirroredField("database_password_1", incoming.getDatabase_password_1(),
                existing.getDatabase_password_1(), incoming::setDatabase_password_1, preserved);

        Map<String, Object> existingConfig = existing.getConfig();
        if (MapUtils.isEmpty(existingConfig)) {
            return preserved;
        }
        Map<String, Object> config = incoming.getConfig();
        if (config == null) {
            config = new LinkedHashMap<>();
            incoming.setConfig(config);
        }
        for (String path : getMaskedConfigPaths(definition)) {
            if (isPresent(getNestedValue(config, path))) {
                continue;
            }
            Object existingValue = getNestedValue(existingConfig, path);
            if (!isPresent(existingValue)) {
                continue;
            }
            setNestedValue(config, path, existingValue);
            preserved.add(path);
        }
        return preserved;
    }

    /** 空字符串与 null 同等对待：脱敏既可能删键，也可能留下空串。 */
    private static boolean isPresent(Object value) {
        return value != null && !(value instanceof CharSequence && ((CharSequence) value).length() == 0);
    }

    /** 顶层镜像字段的逐个补回：包里缺、目标有 ⇒ 用目标的，并记下字段名交给导入报告。 */
    private static <V> void restoreMirroredField(String name, V incomingValue, V existingValue,
            java.util.function.Consumer<V> setter, List<String> preserved) {
        if (isPresent(incomingValue) || !isPresent(existingValue)) {
            return;
        }
        setter.accept(existingValue);
        preserved.add(name);
    }

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
