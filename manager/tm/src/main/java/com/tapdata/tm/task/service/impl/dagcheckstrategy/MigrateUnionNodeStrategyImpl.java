package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;

@Component("migrateUnionNodeStrategy")
public class MigrateUnionNodeStrategyImpl implements DagLogStrategy {
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.MIGRATE_UNION_NODE_CHECK;
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
                .filter(node -> node instanceof MigrateUnionProcessorNode)
                .map(node -> (MigrateUnionProcessorNode) node)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();

                    List<DatabaseNode> sourceDatabaseNodes = getSourceDatabaseNodes(dag, nodeId);
                    boolean enablePartitionTable = sourceDatabaseNodes
                            .stream().filter(n -> n.getSyncSourcePartitionTableEnable() != null && n.getSyncSourcePartitionTableEnable())
                            .findFirst().isPresent();
                    if (enablePartitionTable) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log(MessageUtil.getDagCheckMsg(locale, "MIGRATE_UNION_NOT_SUPPORT_PARTITION_TABLE"))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }

                    if (CollectionUtils.isEmpty(result)) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.INFO).nodeId(nodeId)
                                .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "MIGRATE_UNION_PASS"), name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                });
        return result;
    }

    private List<DatabaseNode> getSourceDatabaseNodes(DAG dag, String nodeId) {
        if (dag == null || nodeId == null) {
            return Collections.emptyList();
        }
        LinkedList<Node<?>> preNodes = dag.getPreNodes(nodeId);

        ArrayList<DatabaseNode> result = new ArrayList<>();
        if (preNodes != null) {
            for (Node<?> node : preNodes) {
                LinkedList<Node<?>> tmp = node.getPreNodes(node.getId());
                if (tmp == null || tmp.isEmpty()) {
                    if (node instanceof DatabaseNode)
                        result.add((DatabaseNode) node);
                } else {
                    return getSourceDatabaseNodes(dag, node.getId());
                }
            }
        }
        return result;
    }
}
