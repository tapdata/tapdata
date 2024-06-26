package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsVo;
import com.tapdata.tm.shareCdcTableMetrics.service.ShareCdcTableMetricsService;
import com.tapdata.tm.task.service.TaskConsoleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.RelationTaskInfoVo;
import com.tapdata.tm.task.vo.RelationTaskRequest;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskConsoleServiceImpl implements TaskConsoleService {
    private TaskService taskService;
    private ShareCdcTableMetricsService shareCdcTableMetricsService;

    private static final String STATUS = "status";
    @Override
    public List<RelationTaskInfoVo> getRelationTasks(RelationTaskRequest request) {
        String taskId = request.getTaskId();
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));

        DAG dag = taskDto.getDag();
        List<Node> nodes = dag.getSources();
        List<String> connectionIds = nodes.stream()
                .filter(node -> node instanceof DataParentNode)
                .map(node -> ((DataParentNode<?>) node).getConnectionId())
                .collect(Collectors.toList());

        List<RelationTaskInfoVo> result = Lists.newArrayList();
        if (RelationTaskRequest.type_logCollector.equals(request.getType())) {
            getLogCollector(connectionIds, result, request, taskDto);
        } else if (RelationTaskRequest.type_shareCache.equals(request.getType())) {
//            getShareCache(connectionIds, result, request, nodes);
        //} else if (RelationTaskRequest.type_inspect.equals(request.getType())) {
        } else if (RelationTaskRequest.type_task_by_collector.equals(request.getType())) {
            getTaskByCollector(result, request, taskDto);
        } else if (RelationTaskRequest.type_task_by_collector_table.equals(request.getType())) {
            getTaskByCollectorTable(result, request, taskDto);
        } else {
            getLogCollector(connectionIds, result, request, taskDto);
//            getShareCache(connectionIds, result, request, nodes);

            result = result.stream().sorted(Comparator.comparing(RelationTaskInfoVo::getStartTime,Comparator.nullsFirst(Long::compareTo)).reversed())
                    .collect(Collectors.toList());
        }
        getShareCacheByTaskAttrs(result, request, taskDto);
        getHeartbeat(result, request, taskDto);
        return result;
    }

    private void getTaskByCollectorTable(List<RelationTaskInfoVo> result, RelationTaskRequest request, TaskDto taskDto) {
        Map<String, Set<String>> tableNameMap = request.getTableNameMap();
        List<Criteria> criteriaList = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : tableNameMap.entrySet()) {
					criteriaList.add(Criteria.where("dag.nodes").elemMatch(new Criteria().andOperator(
									Criteria.where("connectionId").is(entry.getKey()),
									new Criteria().orOperator(Criteria.where("tableName").in(entry.getValue()), Criteria.where("tableNames").in(entry.getValue())))));
        }
        Criteria criteria = new Criteria().andOperator(
                Criteria.where("is_deleted").is(false).and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE),
                new Criteria().orOperator(criteriaList));
        List<TaskDto> logTasks = getFilterCriteria(request, criteria);
        if (CollectionUtils.isEmpty(logTasks)) {
            return;
        }
        List<RelationTaskInfoVo> list = logTasks.stream().map(task -> RelationTaskInfoVo.builder()
                .id(task.getId().toHexString())
                .name(task.getName())
                .taskType(task.getType())
                .syncType(task.getSyncType())
                .status(task.getStatus())
                .build()).collect(Collectors.toList());
        result.addAll(list);
    }

    private void getShareCacheByTaskAttrs(List<RelationTaskInfoVo> result, RelationTaskRequest request, TaskDto taskDto) {
        Criteria cacheCriteria = null;
        if (Boolean.TRUE.equals(taskDto.getShareCache())) {
            cacheCriteria = Criteria.where("is_deleted").is(false)
                    .and("shareCache").is(false)
                    .and(String.format("attrs.%s.%s", TaskDto.ATTRS_USED_SHARE_CACHE, taskDto.getName())).exists(true);
        } else if (StringUtils.isBlank(request.getType())
                || RelationTaskRequest.type_shareCache.equals(request.getType())) {
            Set<String> allCache = Optional.ofNullable(taskDto.getAttrs()).map(m -> {
                Object o = m.get(TaskDto.ATTRS_USED_SHARE_CACHE);
                if (o instanceof Map && !((Map<?, ?>) o).isEmpty()) {
                    return (Map) o;
                }
                return null;
            }).map(m -> {
                Set<String> keys = new HashSet<>();
                for (Object k : m.keySet()) {
                    keys.add(String.valueOf(k));
                }
                return keys;
            }).orElse(null);
            if (null == allCache) return;

            cacheCriteria = Criteria.where("is_deleted").is(false)
                    .and("shareCache").is(true)
                    .and("name").in(allCache);
        }

        if (null != cacheCriteria) {
            for (TaskDto task : getFilterCriteria(request, cacheCriteria)) {
                result.add(RelationTaskInfoVo.builder()
                        .id(task.getId().toHexString())
                        .name(task.getName())
                        .status(task.getStatus())
                        .startTime(Objects.nonNull(task.getStartTime()) ? task.getStartTime().getTime() : null)
                        .type(RelationTaskRequest.type_shareCache)
                        .syncType(task.getSyncType())
                        .taskType(task.getType())
                        .build()
                );
            }
        }
    }

    private void getShareCache(List<String> connectionIds, List<RelationTaskInfoVo> result, RelationTaskRequest request, List<Node> nodes) {
        List<String> tableNames = Lists.newArrayList();
        nodes.forEach(node -> {
            if (node instanceof TableNode) {
                tableNames.add(((TableNode) node).getTableName());
            } else if (node instanceof DatabaseNode) {
                List<SyncObjects> syncObjects = ((DatabaseNode) node).getSyncObjects();
                if (CollectionUtils.isNotEmpty(syncObjects)) {
                    tableNames.addAll(syncObjects.get(0).getObjectNames());
                }
            }
        });

        Criteria cacheCriteria = Criteria.where("is_deleted").is(false).and("syncType").ne("logCollector")
                .and("dag.nodes.type").is(NodeEnum.mem_cache.name())
                .and("dag.nodes.connectionId").in(connectionIds)
                .and("dag.nodes.tableName").in(tableNames);

        List<TaskDto> cacheTasks = getFilterCriteria(request, cacheCriteria);
        cacheTasks.forEach(task -> {
            RelationTaskInfoVo logRelation = RelationTaskInfoVo.builder().id(task.getId().toHexString()).name(task.getName())
                    .status(task.getStatus())
                    .startTime(Objects.nonNull(task.getStartTime()) ? task.getStartTime().getTime() : null)
                    .type(RelationTaskRequest.type_shareCache)
                    .build();

            result.add(logRelation);
        });
    }

    private void getLogCollector(List<String> connectionIds, List<RelationTaskInfoVo> result, RelationTaskRequest request, TaskDto taskDto) {
        if (Objects.nonNull(taskDto.getShareCdcEnable()) && !taskDto.getShareCdcEnable()) {
            return;
        }

        Criteria criteria = Criteria.where("is_deleted").is(false).and("syncType").is("logCollector")
                .and("dag.nodes.type").is(NodeEnum.logCollector.name()).and(STATUS).ne(TaskDto.STATUS_DELETE_FAILED);

        List<Criteria> orCriteriaList = new ArrayList<>();
        orCriteriaList.add(Criteria.where("dag.nodes.connectionIds").in(connectionIds));
        for (String connectionId : connectionIds) {
            orCriteriaList.add(Criteria.where("dag.nodes.logCollectorConnConfigs." + connectionId).exists(true));
        }
        criteria = new Criteria().andOperator(criteria, new Criteria().orOperator(orCriteriaList));

        List<TaskDto> logTasks = getFilterCriteria(request, criteria);
        logTasks.forEach(task -> {
            RelationTaskInfoVo logRelation = RelationTaskInfoVo.builder().id(task.getId().toHexString()).name(task.getName())
                    .status(task.getStatus())
                    .startTime(Objects.nonNull(task.getStartTime()) ? task.getStartTime().getTime() : null)
                    .type(RelationTaskRequest.type_logCollector)
                    .taskType(task.getType())
                    .syncType(task.getSyncType())
                    .build();

            result.add(logRelation);
        });
    }

    private void getHeartbeat(List<RelationTaskInfoVo> result, RelationTaskRequest request, TaskDto taskDto) {
        if (TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(taskDto.getSyncType())) {
            if (null == taskDto.getHeartbeatTasks()) return;
            for (TaskDto task : taskService.findAllTasksByIds(new ArrayList<>(taskDto.getHeartbeatTasks()))) {
                result.add(RelationTaskInfoVo.builder()
                        .id(task.getId().toHexString())
                        .name(task.getName())
                        .status(task.getStatus())
                        .startTime(Objects.nonNull(task.getStartTime()) ? task.getStartTime().getTime() : null)
                        .type(task.getSyncType())
                        .build()
                );
            }
            return;
        }

        if (StringUtils.isBlank(request.getType())
                || RelationTaskRequest.type_ConnHeartbeat.equals(request.getType())
                || RelationTaskRequest.type_task_by_collector.equals(request.getType())) {
            TaskDto heartbeatTaskDto = taskService.findHeartbeatByTaskId(taskDto.getId().toHexString(), "_id", "name", STATUS, "startTime", "syncType");
            if (null == heartbeatTaskDto) return;

            result.add(RelationTaskInfoVo.builder()
                    .id(heartbeatTaskDto.getId().toHexString())
                    .name(heartbeatTaskDto.getName())
                    .status(heartbeatTaskDto.getStatus())
                    .startTime(Objects.nonNull(heartbeatTaskDto.getStartTime()) ? heartbeatTaskDto.getStartTime().getTime() : null)
                    .type(heartbeatTaskDto.getSyncType())
                    .build()
            );
        }
    }

    private void getTaskByCollector(List<RelationTaskInfoVo> result, RelationTaskRequest request, TaskDto taskDto) {
        if (!TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(taskDto.getSyncType())) {
            return;
        }

        List<String> tableNames = null;
        List<ShareCdcTableMetricsVo> collectList = shareCdcTableMetricsService.getCollectInfoByTaskId(request.getTaskId());
        if (CollectionUtils.isNotEmpty(collectList)) {
            tableNames = collectList.stream().map(ShareCdcTableMetricsVo::getTableName).collect(Collectors.toList());
        }

        List<String> connectionIds = taskDto.getDag().getSources().stream().filter(s -> s instanceof LogCollectorNode)
                .flatMap(t -> ((LogCollectorNode) t).getConnectionIds().stream()).collect(Collectors.toList());

        Criteria criteria = Criteria.where("is_deleted").is(false).and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and(STATUS).nin(TaskDto.STATUS_DELETE_FAILED)
                .and("shareCdcEnable").is(true)
                .and("dag.nodes.connectionId").in(connectionIds);

        List<TaskDto> logTasks = getFilterCriteria(request, criteria);
        if (CollectionUtils.isEmpty(logTasks)) {
            return;
        }

        List<String> taskIdList = logTasks.stream().map(t -> t.getId().toHexString()).collect(Collectors.toList());
        List<TaskDto> tasks = taskService.findAllTasksByIds(taskIdList);
        Map<String, TaskDto> taskMap = tasks.stream().collect(Collectors.toMap(t -> t.getId().toHexString(), Function.identity(), (a, b) -> b));

        for (TaskDto task : logTasks) {
            String taskId = task.getId().toHexString();
            RelationTaskInfoVo logRelation = RelationTaskInfoVo.builder().id(taskId)
                    .name(task.getName()).status(task.getStatus())
                    .startTime(Objects.nonNull(task.getStartTime()) ? task.getStartTime().getTime() : null)
                    .build();
            if (taskMap.containsKey(taskId)) {
                TaskDto taskInfo = taskMap.get(taskId);
                boolean match = task.getDag().getSourceNodes().stream().anyMatch(t -> connectionIds.contains(((DataParentNode) t).getConnectionId()));
                if (!match) {
                    continue;
                }
                logRelation.setCurrentEventTimestamp(taskInfo.getCurrentEventTimestamp());
                logRelation.setTaskType(taskInfo.getType());
                logRelation.setCreateDate(taskInfo.getCreateAt());
                logRelation.setSyncType(taskInfo.getShareCache() ? TaskDto.SYNC_TYPE_MEM_CACHE : taskInfo.getSyncType());

                if (Objects.isNull(tableNames)) {
                    logRelation.setTableNum(0);
                } else {
                    List<String> finalTableNames = tableNames;
                    Integer count = taskInfo.getDag().getSourceNodes().stream()
                            .map(node -> {
                                if (node instanceof TableNode) {
                                    if (finalTableNames.contains(((TableNode) node).getTableName())) {
                                        return 1;
                                    }
                                } else if (node instanceof DatabaseNode) {
                                    List<String> names = ((DatabaseNode) node).getTableNames();
                                    names.retainAll(finalTableNames);
                                    return names.size();
                                }
                                return 0;
                            }).reduce(0, Integer::sum);
                    logRelation.setTableNum(count);
                }
            }

            result.add(logRelation);
        }
    }

    private List<TaskDto> getFilterCriteria(RelationTaskRequest request, Criteria criteria) {
        if (StringUtils.isNotBlank(request.getStatus())) {
            criteria.and(STATUS).is(request.getStatus());
        }
        if (StringUtils.isNotBlank(request.getKeyword())) {
            criteria.and("name").regex(request.getKeyword());
        }

        return taskService.findAll(new Query(criteria).with(Sort.by(Sort.Order.desc("startTime"))));
    }
}
