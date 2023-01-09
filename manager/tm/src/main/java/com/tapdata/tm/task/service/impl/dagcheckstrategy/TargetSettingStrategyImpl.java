package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component("targetSettingStrategy")
public class TargetSettingStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_NODE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        ObjectId taskId = taskDto.getId();
        String current = DateUtil.now();
        Date now = new Date();

        List<TaskDagCheckLog> result = Lists.newArrayList();
        Map<String, Integer> nameMap = Maps.newHashMap();
        LinkedList<DatabaseNode> targetNode = taskDto.getDag().getTargetNode();

        if (CollectionUtils.isEmpty(targetNode)) {
            return null;
        }

        targetNode.forEach(node -> {
            String name = node.getName();
            Integer value;
            String template;
            Level grade;
            if (nameMap.containsKey(name)) {
                value = nameMap.get(name) + 1;
                template = templateEnum.getErrorTemplate();
                grade = Level.ERROR;
            } else {
                value = NumberUtils.INTEGER_ZERO;
                template = templateEnum.getInfoTemplate();
                grade = Level.INFO;
            }
            nameMap.put(name, value);

            String content = MessageFormat.format(template, current, name);

            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setTaskId(taskId.toHexString());
            log.setCheckType(templateEnum.name());
            log.setCreateAt(now);
            log.setCreateUser(userDetail.getUserId());
            log.setLog(content);
            log.setGrade(grade);
            log.setNodeId(node.getId());

            result.add(log);
        });

        return result;
    }
}
