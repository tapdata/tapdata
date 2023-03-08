package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Component("unionNodeStrategy")
public class UnionNodeStrategyImpl implements DagLogStrategy {
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.UNION_NODE_CHECK;
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
                .filter(node -> node instanceof UnionProcessorNode)
                .map(node -> (UnionProcessorNode) node)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();

                    if (StringUtils.isEmpty(name)) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log(MessageUtil.getDagCheckMsg(locale, "UNION_NAME_EMPTY"))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }

                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.INFO).nodeId(nodeId)
                                .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "UNION_PASS"), name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                });
        return result;
    }
}
