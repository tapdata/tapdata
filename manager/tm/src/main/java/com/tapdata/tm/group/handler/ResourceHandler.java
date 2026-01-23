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
            DataSourceDefinitionService dataSourceDefinitionService = SpringUtil
                    .getBean(DataSourceDefinitionService.class);
            DataSourceDefinitionDto definition = dataSourceDefinitionService
                    .findByPdkHash(dataSourceConnectionDto.getPdkHash(), Integer.MAX_VALUE, user);
            if (definition != null) {
                dataSourceConnectionDto.setDefinitionPdkAPIVersion(definition.getPdkAPIVersion());
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
            Map<ResourceType, List<MetadataInstancesDto>> metadataList) {
        if (MapUtils.isEmpty(payloads)) {
            return;
        }
        Map<String, Object> connections = (Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.CONNECTION,
                k -> new HashMap<>());
        List<MetadataInstancesDto> connectionMetadata = metadataList.computeIfAbsent(ResourceType.SHARE_CACHE,
                k -> new ArrayList<>());

        // 处理Excel文件（Connections.xlsx）
        List<TaskUpAndLoadDto> excelPayload = payloads.getOrDefault(GroupConstants.COLLECTION_CONNECTION_EXCEL, Collections.emptyList());
        for (TaskUpAndLoadDto taskUpAndLoadDto : excelPayload) {
            byte[] binaryData = taskUpAndLoadDto.getBinaryData();
            if (binaryData != null && binaryData.length > 0) {
                try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(binaryData)) {
                    List<DataSourceConnectionDto> importedConnections = ExcelUtil.importConnectionsFromExcel(bais);
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

}
