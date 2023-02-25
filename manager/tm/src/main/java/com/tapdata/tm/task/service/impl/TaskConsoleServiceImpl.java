package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.TaskConsoleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.RelationTaskInfoVo;
import com.tapdata.tm.task.vo.RelationTaskRequest;
import com.tapdata.tm.utils.CollectionsUtils;
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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskConsoleServiceImpl implements TaskConsoleService {
    private TaskService taskService;
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
        } else {
            getLogCollector(connectionIds, result, request, taskDto);
//            getShareCache(connectionIds, result, request, nodes);

            result = result.stream().sorted(Comparator.nullsFirst(Comparator.comparing(RelationTaskInfoVo::getStartTime).reversed()))
                    .collect(Collectors.toList());
        }
        return result;
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
                .and("dag.nodes.type").is(NodeEnum.logCollector.name())
                .and("dag.nodes.connectionIds").in(connectionIds);

        List<TaskDto> logTasks = getFilterCriteria(request, criteria);
        logTasks.forEach(task -> {
            RelationTaskInfoVo logRelation = RelationTaskInfoVo.builder().id(task.getId().toHexString()).name(task.getName())
                    .status(task.getStatus())
                    .startTime(Objects.nonNull(task.getStartTime()) ? task.getStartTime().getTime() : null)
                    .type(RelationTaskRequest.type_logCollector)
                    .build();

            result.add(logRelation);
        });
    }

    private List<TaskDto> getFilterCriteria(RelationTaskRequest request, Criteria criteria) {
        if (StringUtils.isNotBlank(request.getStatus())) {
            criteria.and("status").is(request.getStatus());
        }
        if (StringUtils.isNotBlank(request.getKeyword())) {
            criteria.and("name").regex(request.getKeyword());
        }

        return taskService.findAll(new Query(criteria).with(Sort.by(Sort.Order.desc("startTime"))));
    }
}
