package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Component("joinNodeStrategy")
public class JoinNodeStrategyImpl implements DagLogStrategy {
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.JOIN_NODE_CHECK;
    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        String taskId = taskDto.getId().toHexString();
        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        Date now = DateUtil.date();
        String userId = userDetail.getUserId();

        List<TaskDagCheckLog> result = Lists.newArrayList();
        dag.getNodes().stream()
                .filter(node -> node instanceof JoinProcessorNode)
                .map(node -> (JoinProcessorNode) node)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();

                    if (StringUtils.isEmpty(name)) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "JOIN_NODE_NAME_EMPTY"));
                    }

                    if (CollectionUtils.isEmpty(node.getJoinExpressions())) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "JOIN_NODE_NOT_SET", name));
                    }

                    if (StringUtils.isEmpty(node.getLeftNodeId()) && StringUtils.isEmpty(node.getRightNodeId())) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "JOIN_NODE_SET_ERROR", name));
                    }

                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.INFO, templateEnum, locale, "JOIN_NODE_PASS", name));
                    }
                });

        return result;
    }
}
