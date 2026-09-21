package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.utils.MessageUtil;

import java.util.Date;
import java.util.Locale;

final class DagCheckLogs {

    private DagCheckLogs() {
    }

    static TaskDagCheckLog of(String taskId, String nodeId, String userId, Date now,
                              Level grade, DagOutputTemplateEnum templateEnum,
                              Locale locale, String resourceId, Object... params) {
        TaskDagCheckLog log = TaskDagCheckLog.builder()
                .taskId(taskId)
                .checkType(templateEnum.name())
                .grade(grade)
                .nodeId(nodeId)
                .log(MessageUtil.getDagCheckMsg(locale, resourceId, params))
                .build();
        log.setCreateAt(now);
        log.setCreateUser(userId);
        return log;
    }

    static MessageUtil.DagNodeName nodeName(Node node) {
        return nodeName(node, node.getName());
    }

    static MessageUtil.DagNodeName nodeName(Node node, String name) {
        return MessageUtil.dagNodeName(node.getType(), name);
    }
}
