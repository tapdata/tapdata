package com.tapdata.tm.group.handler;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 模块资源处理器
 * 处理模块（MODULE）
 *
 */
@Slf4j
@Component
public class ModuleResourceHandler implements ResourceHandler {

    @Autowired
    private ModulesService modulesService;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private MetadataDefinitionService metadataDefinitionService;

    @Override
    public ResourceType getResourceType() {
        return ResourceType.MODULE;
    }

    @Override
    public List<ModulesDto> loadResources(List<String> ids, UserDetail user) {
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        }
        List<ObjectId> objectIds = ids.stream()
                .filter(Objects::nonNull)
                .map(ObjectId::new)
                .collect(Collectors.toList());
        Query query = new Query(Criteria.where("_id").in(objectIds));
        return modulesService.findAllDto(query, user);
    }

    @Override
    public List<TaskUpAndLoadDto> buildExportPayload(List<?> resources, UserDetail user) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        if (CollectionUtils.isEmpty(resources)) {
            return payload;
        }

        
        List<ModulesDto> modules = (List<ModulesDto>) resources;

        for (ModulesDto modulesDto : modules) {
            // 清理敏感和非必要字段
            modulesDto.setCreateUser(null);
            modulesDto.setCustomId(null);
            modulesDto.setLastUpdBy(null);
            modulesDto.setUserId(null);
            payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES, JsonUtil.toJsonUseJackson(modulesDto)));
        }
        return payload;
    }

    @Override
    
    public void collectPayload(List<TaskUpAndLoadDto> payload, Map<String, ?> resourceMap,
            List<MetadataInstancesDto> metadataList) {
        if (CollectionUtils.isEmpty(payload)) {
            return;
        }

        Map<String, ModulesDto> moduleMap = (Map<String, ModulesDto>) resourceMap;

        for (TaskUpAndLoadDto item : payload) {
            if (StringUtils.isBlank(item.getJson())) {
                continue;
            }

            if (GroupConstants.COLLECTION_MODULES.equals(item.getCollectionName())) {
                ModulesDto modulesDto = JsonUtil.parseJsonUseJackson(item.getJson(), ModulesDto.class);
                if (modulesDto != null) {
                    String key = modulesDto.getId() == null ? modulesDto.getName() : modulesDto.getId().toHexString();
                    moduleMap.putIfAbsent(key, modulesDto);
                }
            } else if (GroupConstants.COLLECTION_METADATA_INSTANCES.equals(item.getCollectionName())) {
                MetadataInstancesDto metadataInstancesDto = JsonUtil.parseJsonUseJackson(item.getJson(),
                        MetadataInstancesDto.class);
                if (metadataInstancesDto != null) {
                    metadataList.add(metadataInstancesDto);
                }
            }
        }
    }

    @Override
    public List<DataSourceConnectionDto> loadConnections(List<?> resources) {
        Set<String> connectionIds = new HashSet<>();
        if (CollectionUtils.isEmpty(resources)) {
            return new ArrayList<>();
        }

        
        List<ModulesDto> modules = (List<ModulesDto>) resources;

        for (ModulesDto modulesDto : modules) {
            String connectionId = modulesDto.getConnectionId();
            if (StringUtils.isBlank(connectionId) && modulesDto.getConnection() != null) {
                connectionId = modulesDto.getConnection().toHexString();
            }
            if (StringUtils.isNotBlank(connectionId)) {
                connectionIds.add(connectionId);
            }
        }
        return dataSourceService.findInfoByConnectionIdList(new ArrayList<>(connectionIds));
    }

    @Override
    public Map<String, String> findDuplicateNames(Iterable<?> resources, UserDetail user) {
        Map<String, String> duplicates = new HashMap<>();

        
        Iterable<ModulesDto> modules = (Iterable<ModulesDto>) resources;

        for (ModulesDto modulesDto : modules) {
            if (modulesDto == null || StringUtils.isBlank(modulesDto.getName())) {
                continue;
            }
            if (duplicates.containsKey(modulesDto.getName())) {
                continue;
            }
            Query query = new Query(Criteria.where("name").is(modulesDto.getName())
                    .and("is_deleted").ne(true));
            query.fields().include("_id", "name");
            ModulesDto existing = modulesService.findOne(query, user);
            if (existing != null) {
                duplicates.put(modulesDto.getName(), "duplicate");
            }
        }
        return duplicates;
    }

    @Override
    public String resolveResourceName(String resourceId, Map<String, ?> resourceMap) {
        if (resourceMap == null || resourceId == null) {
            return null;
        }

        Map<String, ModulesDto> moduleMap = (Map<String, ModulesDto>) resourceMap;

        ModulesDto modulesDto = moduleMap.get(resourceId);
        return modulesDto == null ? null : modulesDto.getName();
    }

    @Override
    public void handleRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloadsByType, List<?> resources,
            UserDetail user,Set<ObjectId> tagIds) {
        ResourceHandler.super.handleRelatedResources(payloadsByType, resources, user,tagIds);

        List<ModulesDto> modules = (List<ModulesDto>) resources;
        for (ModulesDto modulesDto : modules) {
            if (CollectionUtils.isNotEmpty(modulesDto.getListtags())) {
                tagIds.addAll(modulesDto.getListtags().stream()
                        .map(tag -> MongoUtils.toObjectId(tag.getId()))
                        .toList());
            }
        }
        if (CollectionUtils.isNotEmpty(tagIds)) {
            List<MetadataDefinitionDto> allDto = metadataDefinitionService.findAndParent(null, tagIds.stream().toList());
            if (CollectionUtils.isNotEmpty(allDto)) {
                List<TaskUpAndLoadDto> payload = new ArrayList<>(allDto.stream()
                        .map(t -> new TaskUpAndLoadDto(GroupConstants.METADATA_DEFINITION, JsonUtil.toJsonUseJackson(t)))
                        .toList());
                payloadsByType.computeIfAbsent(ResourceType.METADATA_DEFINITION.name(), k -> new ArrayList<>()).addAll(payload);
            }
        }
    }
}
