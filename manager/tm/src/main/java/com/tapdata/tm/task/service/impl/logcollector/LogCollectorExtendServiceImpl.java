package com.tapdata.tm.task.service.impl.logcollector;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.LogCollectorExtendService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.LogCollectorRelateTaskVo;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Setter(onMethod_ = {@Autowired})
public class LogCollectorExtendServiceImpl implements LogCollectorExtendService {
    private TaskService taskService;

    @Override
    public Page<LogCollectorRelateTaskVo> getRelationTask(String taskId, String type, Integer page, Integer size) {
        Criteria taskCriteria;
        if (NodeEnum.logCollector.name().equals(type)) {
            taskCriteria = getLogCollectorCriteria(taskId);
        } else if (NodeEnum.mem_cache.name().equals(type)) {
            taskCriteria = getShareCacheCriteria(taskId);
        } else {
            return new Page<>(0, Lists.newArrayList());
        }

        TmPageable tmPageable = new TmPageable();
        tmPageable.setPage(page);
        tmPageable.setSize(size);
        Query query = new Query(taskCriteria);

        long count = taskService.count(query);
        if (count == 0) {
            return new Page<>(0, Lists.newArrayList());
        }

        List<TaskDto> taskDtos = taskService.findAll(query.with(tmPageable));
        List<LogCollectorRelateTaskVo> list = Lists.newArrayList();
        for (TaskDto dto : taskDtos) {
            LogCollectorRelateTaskVo build = LogCollectorRelateTaskVo.builder().taskId(dto.getId().toHexString())
                    .name(dto.getName()).type(dto.getType()).status(dto.getStatus()).creatTime(dto.getCreateAt().getTime())
                    .syncType(dto.getSyncType())
                    .build();
            list.add(build);
        }
        return new Page<>(count, list);
    }

    private Criteria getShareCacheCriteria(String taskId) {
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        DAG dag = taskDto.getDag();
        List<String> connectionIds = Lists.newArrayList();
        List<String> tableNames = Lists.newArrayList();
        dag.getNodes().stream().filter(node -> node instanceof TableNode)
                .forEach(node -> {
                    connectionIds.add(((TableNode) node).getConnectionId());
                    tableNames.add(((TableNode) node).getTableName());
                });

        return Criteria.where("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and("is_deleted").is(false)
                .and("dag.nodes.type").ne(NodeEnum.mem_cache.name())
                .and("dag.nodes.connectionId").in(connectionIds)
                .and("dag.nodes.syncObjects.objectNames").in(tableNames);
    }

    @NotNull
    private Criteria getLogCollectorCriteria(String taskId) {
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        DAG dag = taskDto.getDag();
        List<String> connectionIds = Lists.newArrayList();
        dag.getNodes().stream().filter(node -> node instanceof LogCollectorNode)
                .forEach(node -> connectionIds.addAll(((LogCollectorNode) node).getConnectionIds()));

        return Criteria.where("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and("shareCdcEnable").is(true)
                .and("is_deleted").is(false)
                .and("dag.nodes.type").ne(NodeEnum.mem_cache.name())
                .and("dag.nodes.connectionId").in(connectionIds);
    }
}
