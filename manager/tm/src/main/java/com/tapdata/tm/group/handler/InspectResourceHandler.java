package com.tapdata.tm.group.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
public class InspectResourceHandler implements ResourceHandler {

    @Autowired
    InspectService inspectService;

    @Override
    public ResourceType getResourceType() {
        return ResourceType.INSPECT_TASK;
    }

    @Override
    public List<?> loadResources(List<String> ids, UserDetail user) {
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        }
        List<ObjectId> objectIds = ids.stream()
                .filter(Objects::nonNull)
                .map(ObjectId::new)
                .collect(Collectors.toList());
        Query query = new Query(Criteria.where("_id").in(objectIds));
        return inspectService.findAllDto(query, user);
    }

    @Override
    public List<TaskUpAndLoadDto> buildExportPayload(List<?> resources, UserDetail user) {
        if (CollectionUtils.isEmpty(resources)) {
            return new ArrayList<>();
        }
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        List<InspectDto> inspectDtos = (List<InspectDto>) resources;
        for (InspectDto inspectDto : inspectDtos) {
            cleanInspectDtoForExport(inspectDto);
            Map<String, Object> jsonMap = JsonUtil.parseJsonUseJackson(
                    JsonUtil.toJsonUseJackson(inspectDto), new TypeReference<Map<String, Object>>() {});
            if (jsonMap != null) {
                jsonMap.remove("createTime");
                jsonMap.remove("last_updated");
            }
            payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_INSPECT, JsonUtil.toJsonUseJackson(jsonMap)));
        }
        return payload;
    }

    /**
     * 清理校验任务中不需要导出的字段：用户相关字段、状态相关字段、运行时字段
     */
    public void cleanInspectDtoForExport(InspectDto inspectDto) {
        // 清理用户相关字段
        inspectDto.setCreateUser(null);
        inspectDto.setCustomId(null);
        inspectDto.setLastUpdBy(null);
        inspectDto.setUserId(null);
        // 清理调度相关字段
        inspectDto.setAgentId(null);
        inspectDto.setHostName(null);
        inspectDto.setAgentName(null);
        inspectDto.setAgentTags(null);
        inspectDto.setScheduleTimes(null);
        inspectDto.setScheduleTime(null);
        // 清理状态相关字段
        inspectDto.setStatus(null);
        inspectDto.setPing_time(null);
        inspectDto.setLastStartTime(null);
        inspectDto.setErrorMsg(null);
        inspectDto.setResult(null);
        inspectDto.setDifferenceNumber(0);
        inspectDto.setInspectResult(null);
        inspectDto.setVersion(null);
        // 清理运行时字段
        inspectDto.setInspectResultId(null);
        inspectDto.setTaskIds(null);
        inspectDto.setCanRecovery(null);
        inspectDto.setCreateAt(null);
        inspectDto.setLastUpdAt(null);
        inspectDto.setPlatformInfo(null);
        inspectDto.setCreateAt(null);
        inspectDto.setCreateUser(null);

    }

    @Override
    public void collectPayload(List<TaskUpAndLoadDto> payload, Map<String, ?> resourceMap,
            List<MetadataInstancesDto> metadataList) {
        if (CollectionUtils.isEmpty(payload)) {
            return;
        }
        Map<String, InspectDto> inspectMap = (Map<String, InspectDto>) resourceMap;
        for (TaskUpAndLoadDto item : payload) {
            if (StringUtils.isBlank(item.getJson())) {
                continue;
            }
            if (GroupConstants.COLLECTION_INSPECT.equals(item.getCollectionName())) {
                InspectDto inspectDto = JsonUtil.parseJsonUseJackson(item.getJson(), InspectDto.class);
                if (inspectDto != null) {
                    String key = inspectDto.getId() == null ? inspectDto.getName() : inspectDto.getId().toHexString();
                    inspectMap.putIfAbsent(key, inspectDto);
                }
            }
        }
    }

    @Override
    public List<DataSourceConnectionDto> loadConnections(List<?> resources) {
        return List.of();
    }

    @Override
    public Map<String, String> findDuplicateNames(Iterable<?> resources, UserDetail user) {
        Map<String, String> duplicates = new HashMap<>();
        Iterable<InspectDto> inspects = (Iterable<InspectDto>) resources;
        for (InspectDto inspectDto : inspects) {
            if (inspectDto == null || StringUtils.isBlank(inspectDto.getName())) {
                continue;
            }
            if (duplicates.containsKey(inspectDto.getName())) {
                continue;
            }
            Query query = new Query(Criteria.where("name").is(inspectDto.getName())
                    .and("is_deleted").ne(true));
            query.fields().include("_id", "name");
            InspectDto existing = inspectService.findOne(query, user);
            if (existing != null) {
                duplicates.put(inspectDto.getName(), GroupConstants.DUPLICATE_MARKER);
            }
        }
        return duplicates;
    }

    @Override
    public String resolveResourceName(String resourceId, Map<String, ?> resourceMap) {
        if (resourceMap == null || resourceId == null) {
            return null;
        }
        Map<String, InspectDto> inspectMap = (Map<String, InspectDto>) resourceMap;
        InspectDto inspectDto = inspectMap.get(resourceId);
        return inspectDto == null ? null : inspectDto.getName();
    }
}
