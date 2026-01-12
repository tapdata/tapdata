package com.tapdata.tm.group.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

/**
 * 资源处理器接口
 * 定义分组模块中各种资源类型的统一处理规范
 * 支持资源的加载、导出、导入、校验等操作
 *
 */
public interface ResourceHandler {

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
            UserDetail user) {
        List<DataSourceConnectionDto> connections = loadConnections(resources);
        List<TaskUpAndLoadDto> connectionPayload = buildConnectionPayload(connections, user);
        payloadsByType.computeIfAbsent(ResourceType.CONNECTION.name(), k -> new ArrayList<>())
                .addAll(connectionPayload);
    }

    default List<TaskUpAndLoadDto> buildConnectionPayload(List<DataSourceConnectionDto> connections, UserDetail user) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        if (CollectionUtils.isEmpty(connections)) {
            return payload;
        }
        for (DataSourceConnectionDto dataSourceConnectionDto : connections) {
            Map<String, Object> config = dataSourceConnectionDto.getConfig();
            if (config != null) {
                config.forEach((k, v) -> {
                    if (GroupConstants.MASK_PROPERTIES.contains(k)) {
                        config.put(k, "");
                    }
                });
            }
            dataSourceConnectionDto.setConnectionString(null);
            dataSourceConnectionDto.setCreateUser(null);
            dataSourceConnectionDto.setCustomId(null);
            dataSourceConnectionDto.setLastUpdBy(null);
            dataSourceConnectionDto.setUserId(null);
            dataSourceConnectionDto.setListtags(null);
            DataSourceDefinitionService dataSourceDefinitionService = SpringUtil
                    .getBean(DataSourceDefinitionService.class);
            DataSourceDefinitionDto definition = dataSourceDefinitionService
                    .findByPdkHash(dataSourceConnectionDto.getPdkHash(), Integer.MAX_VALUE, user);
            if (definition != null) {
                dataSourceConnectionDto.setDefinitionPdkAPIVersion(definition.getPdkAPIVersion());
            }
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
            payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_CONNECTION,
                    JsonUtil.toJsonUseJackson(dataSourceConnectionDto)));
        }
        return payload;
    }

    default void collectPayloadRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloads,
            Map<ResourceType, Map<String, ?>> resourceMap,
            Map<ResourceType, List<MetadataInstancesDto>> metadataList) {
        if (MapUtils.isEmpty(payloads)) {
            return;
        }
        Map<String, Object> connections = (Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.CONNECTION,
                k -> new HashMap<>());
        List<MetadataInstancesDto> connectionMetadata = metadataList.computeIfAbsent(ResourceType.SHARE_CACHE,
                k -> new ArrayList<>());
        String filename = ResourceType.getResourceName(ResourceType.CONNECTION.name());
        List<TaskUpAndLoadDto> payload = payloads.getOrDefault(filename, Collections.emptyList());
        for (TaskUpAndLoadDto taskUpAndLoadDto : payload) {
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
    }

}
