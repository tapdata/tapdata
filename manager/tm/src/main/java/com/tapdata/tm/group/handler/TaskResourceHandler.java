package com.tapdata.tm.group.handler;

import cn.hutool.core.date.DateUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.constant.SyncStatus;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 任务资源处理器
 * 处理迁移任务（MIGRATE_TASK）和同步任务（SYNC_TASK）
 *
 */
@Slf4j
@Component
public class TaskResourceHandler implements ResourceHandler {

    @Autowired
    private TaskService taskService;

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private InspectService inspectService;

    @Autowired
    private ExternalStorageService externalStorageService;

    @Autowired
    private MetadataDefinitionService metadataDefinitionService;

    private final ResourceType resourceType;

    /**
     * 默认构造函数，支持所有任务类型（MIGRATE_TASK 和 SYNC_TASK）
     */
    public TaskResourceHandler() {
        this.resourceType = null; // null 表示支持所有任务类型
    }

    /**
     * 带参构造函数，支持指定的任务类型
     * 
     * @param resourceType 资源类型（MIGRATE_TASK 或 SYNC_TASK）
     */
    public TaskResourceHandler(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    @Override
    public ResourceType getResourceType() {
        return resourceType;
    }

    /**
     * 判断是否支持指定的资源类型
     */
    public boolean supports(ResourceType type) {
        return type == ResourceType.MIGRATE_TASK || type == ResourceType.SYNC_TASK;
    }

    @Override
    public List<TaskDto> loadResources(List<String> ids, UserDetail user) {
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        }
        List<ObjectId> objectIds = ids.stream()
                .filter(Objects::nonNull)
                .map(MongoUtils::toObjectId)
                .collect(Collectors.toList());
        Query query = new Query(Criteria.where("_id").in(objectIds));
        return taskService.findAllDto(query, user);
    }

    @Override
    public List<TaskUpAndLoadDto> buildExportPayload(List<?> resources, UserDetail user) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        if (CollectionUtils.isEmpty(resources)) {
            return payload;
        }

        List<TaskDto> tasks = (List<TaskDto>) resources;
        Map<String,String> externalStorageMap = new HashMap<>();

        for (TaskDto taskDto : tasks) {
            // 清理敏感和非必要字段
            taskDto.setCreateUser(null);
            taskDto.setCustomId(null);
            taskDto.setLastUpdBy(null);
            taskDto.setUserId(null);
            taskDto.setAgentId(null);
            taskDto.setStatus(TaskDto.STATUS_EDIT);
            taskDto.setSyncStatus(SyncStatus.NORMAL);
            taskDto.setStatuses(new ArrayList<>());
            taskDto.setAttrs(new HashMap<>());
            taskDto.setScheduledTime(null);
            taskDto.setSchedulingTime(null);
            taskDto.setRunningTime(null);
            taskDto.setErrorTime(null);
            taskDto.setPingTime(null);
            taskDto.setMetricInfo(null);
            taskDto.setLdpNewTables(null);
            taskDto.setStopTime(null);
            taskDto.setStartTime(null);
            taskDto.setStopedDate(null);
            taskDto.setStoppingTime(null);
            taskDto.setSnapshotDoneAt(null);
            // 导出任务关联的元数据
            DAG dag = taskDto.getDag();
            if (dag == null || CollectionUtils.isEmpty(dag.getNodes())) {
                continue;
            }
            dag.getNodes().forEach(node -> {
                if(StringUtils.isNotBlank(node.getExternalStorageId())){
                    if(externalStorageMap.containsKey(node.getExternalStorageId())){
                        node.setExternalStorageName(externalStorageMap.get(node.getExternalStorageId()));
                    }else{
                        ExternalStorageDto externalStorageDto = externalStorageService.findById(MongoUtils.toObjectId(node.getExternalStorageId()), Field.includes("name"));
                        if(null != externalStorageDto){
                            node.setExternalStorageName(externalStorageDto.getName());
                            externalStorageMap.put(node.getExternalStorageId(), externalStorageDto.getName());
                        }
                    }
                }
            });

            payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_TASK, JsonUtil.toJsonUseJackson(taskDto)));

            int metadataCount = 0;
            try {
                for (Node<?> node : dag.getNodes()) {
                    List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService
                            .findByNodeId(node.getId(), null, user, taskDto);
                    if (CollectionUtils.isEmpty(metadataInstancesDtos)) {
                        continue;
                    }
                    for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
                        metadataInstancesDto.setCreateUser(null);
                        metadataInstancesDto.setCustomId(null);
                        metadataInstancesDto.setLastUpdBy(null);
                        metadataInstancesDto.setUserId(null);
                        payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_METADATA_INSTANCES,
                                JsonUtil.toJsonUseJackson(metadataInstancesDto)));
                        metadataCount++;
                    }
                }
                log.debug("Export task metadata succeeded, taskId={}, taskName={}, nodeCount={}, metadataCount={}",
                        taskDto.getId(), taskDto.getName(), dag.getNodes().size(), metadataCount);
            } catch (Exception e) {
                log.error("Export task node metadata failed, taskId={}, taskName={}, error={}",
                        taskDto.getId(), taskDto.getName(), e.getMessage(), e);
            }
        }
        return payload;
    }

    @Override
    public void collectPayload(List<TaskUpAndLoadDto> payload, Map<String, ?> resourceMap,
            List<MetadataInstancesDto> metadataList) {
        if (CollectionUtils.isEmpty(payload)) {
            return;
        }

        Map<String, TaskDto> taskMap = (Map<String, TaskDto>) resourceMap;

        for (TaskUpAndLoadDto item : payload) {
            if (StringUtils.isBlank(item.getJson())) {
                continue;
            }

            if (GroupConstants.COLLECTION_TASK.equals(item.getCollectionName())) {
                TaskDto taskDto = JsonUtil.parseJsonUseJackson(item.getJson(), TaskDto.class);
                if (taskDto != null) {
                    String key = taskDto.getId() == null ? taskDto.getName() : taskDto.getId().toHexString();
                    taskMap.putIfAbsent(key, taskDto);
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

        List<TaskDto> tasks = (List<TaskDto>) resources;

        for (TaskDto taskDto : tasks) {
            DAG dag = taskDto.getDag();
            if (dag == null || CollectionUtils.isEmpty(dag.getNodes())) {
                continue;
            }
            for (Node<?> node : dag.getNodes()) {
                if (node instanceof DataParentNode) {
                    String connectionId = ((DataParentNode<?>) node).getConnectionId();
                    if (StringUtils.isNotBlank(connectionId)) {
                        connectionIds.add(connectionId);
                    }
                }
            }
        }
        return dataSourceService.findInfoByConnectionIdList(new ArrayList<>(connectionIds));

    }

    @Override
    public Map<String, String> findDuplicateNames(Iterable<?> resources, UserDetail user) {
        Map<String, String> duplicates = new HashMap<>();

        Iterable<TaskDto> tasks = (Iterable<TaskDto>) resources;

        for (TaskDto taskDto : tasks) {
            if (taskDto == null || StringUtils.isBlank(taskDto.getName())) {
                continue;
            }
            if (duplicates.containsKey(taskDto.getName())) {
                continue;
            }
            Query query = new Query(Criteria.where("name").is(taskDto.getName())
                    .and("is_deleted").ne(true));
            query.fields().include("_id", "name");
            TaskDto existing = taskService.findOne(query, user);
            if (existing != null) {
                duplicates.put(taskDto.getName(), "duplicate");
            }
        }
        return duplicates;
    }

    @Override
    public String resolveResourceName(String resourceId, Map<String, ?> resourceMap) {
        if (resourceMap == null || resourceId == null) {
            return null;
        }

        Map<String, TaskDto> taskMap = (Map<String, TaskDto>) resourceMap;

        TaskDto taskDto = taskMap.get(resourceId);
        return taskDto == null ? null : taskDto.getName();
    }

    @Override
    public void handleRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloadsByType, List<?> resources,
            UserDetail user,Set<ObjectId> tagIds) {
        ResourceHandler.super.handleRelatedResources(payloadsByType, resources, user,tagIds);
        List<TaskDto> tasks = (List<TaskDto>) resources;
        Set<String> shareCacheNames = new HashSet<>();
        for (TaskDto taskDto : tasks) {
            if(CollectionUtils.isNotEmpty(taskDto.getListtags())){
                tagIds.addAll(taskDto.getListtags().stream().map(tag -> MongoUtils.toObjectId(tag.getId())).toList());
            }
            if (null == taskDto.getAttrs()) {
                continue;
            }
            Map<String, List<String>> usedShareCache = (Map<String, List<String>>) taskDto.getAttrs()
                    .get("usedShareCache");
            if (MapUtils.isNotEmpty(usedShareCache)) {
                shareCacheNames.addAll(usedShareCache.keySet());
            }
        }
        if (CollectionUtils.isNotEmpty(shareCacheNames)) {
            List<TaskDto> shareCacheTasks = taskService.findAllDto(
                    Query.query(Criteria.where("name").in(shareCacheNames).and("is_deleted").ne(true)),
                    user);
            List<TaskUpAndLoadDto> payload = buildExportPayload(shareCacheTasks, user);
            payloadsByType.computeIfAbsent(ResourceType.SHARE_CACHE.name(), k -> new ArrayList<>()).addAll(payload);
        }

        List<InspectDto> inspectDtoList = inspectService
                .findByTaskIdList(tasks.stream().map(t -> t.getId().toHexString()).collect(Collectors.toList()));
        if (CollectionUtils.isNotEmpty(inspectDtoList)) {
            inspectDtoList.forEach(inspectDto -> {
                if(CollectionUtils.isNotEmpty(inspectDto.getListtags())){
                    tagIds.addAll(inspectDto.getListtags().stream().map(tag -> MongoUtils.toObjectId(tag.getId())).toList());
                }
            });
            List<TaskUpAndLoadDto> payload = new ArrayList<>(inspectDtoList.stream()
                    .map(t -> new TaskUpAndLoadDto(GroupConstants.COLLECTION_INSPECT, JsonUtil.toJsonUseJackson(t)))
                    .toList());
            payloadsByType.computeIfAbsent(ResourceType.INSPECT_TASK.name(), k -> new ArrayList<>()).addAll(payload);
        }

        List<MetadataDefinitionDto> allDto = metadataDefinitionService.findAndParent(null, tagIds.stream().toList());
        if (CollectionUtils.isNotEmpty(allDto)) {
            List<TaskUpAndLoadDto> payload = new ArrayList<>(allDto.stream()
                    .map(t -> new TaskUpAndLoadDto(GroupConstants.METADATA_DEFINITION, JsonUtil.toJsonUseJackson(t)))
                    .toList());
            payloadsByType.computeIfAbsent(ResourceType.METADATA_DEFINITION.name(), k -> new ArrayList<>()).addAll(payload);
        }

    }

    @Override
    public void collectPayloadRelatedResources(Map<String, List<TaskUpAndLoadDto>> payloads,
            Map<ResourceType, Map<String, ?>> resourceMap,
            Map<ResourceType, List<MetadataInstancesDto>> metadataList) {
        ResourceHandler.super.collectPayloadRelatedResources(payloads, resourceMap, metadataList);
        String shareCacheFilename = ResourceType.getResourceName(ResourceType.SHARE_CACHE.name());
        List<TaskUpAndLoadDto> sharCachePayload = payloads.getOrDefault(shareCacheFilename, Collections.emptyList());
        collectPayload(sharCachePayload, resourceMap.computeIfAbsent(ResourceType.SHARE_CACHE, k -> new HashMap<>()),
                metadataList.computeIfAbsent(ResourceType.SHARE_CACHE, k -> new ArrayList<>()));
        String inspectFilename = ResourceType.getResourceName(ResourceType.INSPECT_TASK.name());
        List<TaskUpAndLoadDto> inspectPayload = payloads.getOrDefault(inspectFilename, Collections.emptyList());
        Map<String, Object> inspectMap = (Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.INSPECT_TASK, k -> new HashMap<>());
        for (TaskUpAndLoadDto taskUpAndLoadDto : inspectPayload) {
            if (StringUtils.isBlank(taskUpAndLoadDto.getJson())) {
                continue;
            }
            InspectDto inspectDto = JsonUtil.parseJsonUseJackson(taskUpAndLoadDto.getJson(), InspectDto.class);
            if (inspectDto != null) {
                inspectMap.putIfAbsent(inspectDto.getId().toHexString(), inspectDto);
            }
        }
    }
}
