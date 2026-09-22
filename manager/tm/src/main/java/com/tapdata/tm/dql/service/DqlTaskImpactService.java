package com.tapdata.tm.dql.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.vo.DqlTaskImpactRequestVo;
import com.tapdata.tm.dql.vo.DqlTaskImpactVo;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class DqlTaskImpactService {
    private final TaskService taskService;
    private final DqlEventRepository eventRepository;

    @Autowired
    public DqlTaskImpactService(TaskService taskService, DqlEventRepository eventRepository) {
        this.taskService = taskService;
        this.eventRepository = eventRepository;
    }

    public List<DqlTaskImpactVo> check(DqlTaskImpactRequestVo request, UserDetail userDetail) {
        List<String> taskIds = distinctTaskIds(request);
        if (taskIds.isEmpty()) {
            return List.of();
        }

        Map<String, TaskEntity> visibleTasks = findVisibleTasks(taskIds, userDetail);
        Map<String, Long> taskVersions = visibleTasks.values().stream()
                .filter(task -> task.getId() != null && task.getVersion() != null)
                .collect(Collectors.toMap(
                        task -> task.getId().toHexString(),
                        TaskEntity::getVersion,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));
        Map<String, Long> eventCounts = eventRepository.countByTaskIdAndVersion(taskVersions);

        return taskIds.stream()
                .map(taskId -> {
                    TaskEntity task = visibleTasks.get(taskId);
                    return new DqlTaskImpactVo(
                            taskId,
                            task != null,
                            task == null ? 0L : eventCounts.getOrDefault(taskId, 0L)
                    );
                })
                .toList();
    }

    private List<String> distinctTaskIds(DqlTaskImpactRequestVo request) {
        if (request == null || request.getTaskIds() == null) {
            return List.of();
        }
        return request.getTaskIds().stream()
                .filter(StringUtils::isNotBlank)
                .map(String::trim)
                .distinct()
                .toList();
    }

    private Map<String, TaskEntity> findVisibleTasks(List<String> taskIds, UserDetail userDetail) {
        List<ObjectId> objectIds = taskIds.stream()
                .filter(ObjectId::isValid)
                .map(ObjectId::new)
                .toList();
        if (objectIds.isEmpty()) {
            return Map.of();
        }

        Query query = Query.query(Criteria.where("_id").in(objectIds)
                .and("is_deleted").ne(true));
        query.fields().include("_id").include("version");
        List<TaskEntity> tasks = taskService.findAll(query, userDetail);
        if (tasks == null || tasks.isEmpty()) {
            return Map.of();
        }
        return tasks.stream()
                .filter(task -> task.getId() != null)
                .collect(Collectors.toMap(
                        task -> task.getId().toHexString(),
                        Function.identity(),
                        (left, right) -> left
                ));
    }
}
