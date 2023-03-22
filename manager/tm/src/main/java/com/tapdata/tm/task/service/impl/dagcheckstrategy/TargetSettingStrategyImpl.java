package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Sets;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Component("targetSettingStrategy")
public class TargetSettingStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_NODE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        String taskId = taskDto.getId().toHexString();
        String current = DateUtil.now();
        Date now = new Date();

        List<TaskDagCheckLog> result = Lists.newArrayList();
        Set<String> nameSet = Sets.newHashSet();
        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getTargets())) {
            return null;
        }

        String userId = userDetail.getUserId();
        dag.getTargets().forEach(node -> {
            String name = node.getName();
            String nodeId = node.getId();

            DataParentNode dataParentNode = (DataParentNode) node;

            if (StringUtils.isEmpty(name)) {
                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                        .grade(Level.ERROR).nodeId(nodeId)
                        .log(MessageFormat.format("$date【$taskName】【目标节点设置检测】：目标节点{0}节点名称为空。", dataParentNode.getDatabaseType()))
                        .build();
                log.setCreateAt(now);
                log.setCreateUser(userId);
                result.add(log);
            }

            if (StringUtils.isEmpty(dataParentNode.getConnectionId())) {
                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                        .grade(Level.ERROR).nodeId(nodeId)
                        .log(MessageFormat.format("$date【$taskName】【目标节点设置检测】：目标节点{0}未选择数据库。", name))
                        .build();
                log.setCreateAt(now);
                log.setCreateUser(userId);
                result.add(log);
            }

            AtomicReference<List<String>> tableNames = new AtomicReference<>();
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                Optional.ofNullable(databaseNode.getSyncObjects()).ifPresent(list -> tableNames.set(list.get(0).getObjectNames()));
            } else {
                TableNode tableNode = (TableNode) node;
                tableNames.set(Lists.newArrayList(tableNode.getTableName()));
                if ("updateOrInsert".equals(tableNode.getWriteStrategy()) && CollectionUtils.isEmpty(tableNode.getUpdateConditionFields())) {
                    TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                            .grade(Level.ERROR).nodeId(nodeId)
                            .log(MessageFormat.format("$date【$taskName】【目标节点设置检测】：目标节点{0}更新条件字段未设置。", name))
                            .build();
                    log.setCreateAt(now);
                    log.setCreateUser(userId);
                    result.add(log);
                }
            }

            if (CollectionUtils.isEmpty(tableNames.get())) {
                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                        .grade(Level.ERROR).nodeId(nodeId)
                        .log(MessageFormat.format("$date【$taskName】【目标节点设置检测】：目标节点{0}未选择表。", name))
                        .build();
                log.setCreateAt(now);
                log.setCreateUser(userId);
                result.add(log);
            }

            Integer value;
            String template;
            Level grade;
            if (nameSet.contains(name)) {
                template = templateEnum.getErrorTemplate();
                grade = Level.ERROR;
            } else {
                template = templateEnum.getInfoTemplate();
                grade = Level.INFO;
            }
            nameSet.add(name);

            String content = MessageFormat.format(template, current, name);

            TaskDagCheckLog log = TaskDagCheckLog.builder()
                    .taskId(taskId)
                    .checkType(templateEnum.name())
                    .log(content)
                    .grade(grade)
                    .nodeId(nodeId).build();

            log.setCreateAt(now);
            log.setCreateUser(userId);
            result.add(log);
        });

        return result;
    }
}
