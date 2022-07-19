package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Component("fieldEditStrategy")
public class FieldEditStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.FIELD_EDIT_NODE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        ObjectId taskId = taskDto.getId();
        String taskName = taskDto.getName();
        String current = DateUtil.now();
        Date now = new Date();

        List<TaskDagCheckLog> result = Lists.newArrayList();
        List<MigrateFieldRenameProcessorNode> collect = taskDto.getDag().getNodes().stream()
                .filter(node -> node instanceof MigrateFieldRenameProcessorNode)
                .map(node -> (MigrateFieldRenameProcessorNode) node)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(collect)) {
            return Lists.newArrayList();
        }

        collect.forEach(node -> {
            //String name = node.getName();
            //Integer value;
            //String template;
            //if (nameMap.containsKey(name)) {
            //    value = nameMap.get(name) + 1;
            //    template = templateEnum.getErrorTemplate();
            //} else {
            //    value = NumberUtils.INTEGER_ZERO;
            //    template = templateEnum.getInfoTemplate();
            //}
            //nameMap.put(name, value);
            //
            //String content = String.format(template, current, taskName, name);
            //
            //TaskDagCheckLog log = new TaskDagCheckLog();
            //log.setTaskId(taskId.toHexString());
            //log.setCheckType(templateEnum.name());
            //log.setCreateAt(now);
            //log.setCreateUser(userDetail.getUserId());
            //log.setLog(content);
            //
            //result.add(log);
        });

        return result;
    }
}
