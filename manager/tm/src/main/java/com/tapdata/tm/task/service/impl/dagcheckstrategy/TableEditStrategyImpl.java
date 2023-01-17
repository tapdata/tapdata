package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Component("tableEditStrategy")
public class TableEditStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TABLE_EDIT_NODE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        ObjectId taskId = taskDto.getId();
        String current = DateUtil.now();
        Date now = new Date();

        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        List<Node> collect = dag.getNodes().stream()
                .filter(node -> node instanceof TableRenameProcessNode)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(collect)) {
            return null;
        }

        List<TaskDagCheckLog> result = Lists.newArrayList();
        collect.forEach(node -> {
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setTaskId(taskId.toHexString());
            log.setCheckType(templateEnum.name());
            log.setCreateAt(now);
            log.setCreateUser(userDetail.getUserId());
            log.setLog(MessageFormat.format(templateEnum.getInfoTemplate(), current, node.getName()));
            log.setGrade(Level.INFO);
            log.setNodeId(node.getId());

            result.add(log);
        });

        return result;
    }
}
