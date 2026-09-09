package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
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

@Component("mergeTableNodeStrategy")
public class MergeTableNodeStrategyImpl implements DagLogStrategy {
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.MERGE_NODE_CHECK;
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
                .filter(node -> node instanceof MergeTableNode)
                .map(node -> (MergeTableNode) node)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();

                    if (StringUtils.isEmpty(name)) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "MERGE_TABLE_NAME_EMPTY"));
                    }

                    Boolean noRelate = false;
                    List<MergeTableProperties> mergeProperties = node.getMergeProperties();
                    if (CollectionUtils.isEmpty(mergeProperties)) {
                        noRelate = true;
                    } else {
                        for (MergeTableProperties prop : mergeProperties) {
                            noRelate = checkRelateIsEmpty(prop.getChildren());
                        }
                    }

                    if (noRelate) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "MERGE_TABLE_RELATE_EMPTY", DagCheckLogs.nodeName(node)));
                    }

                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.INFO, templateEnum, locale, "MERGE_TABLE_PASS", DagCheckLogs.nodeName(node)));
                    }
                });
        return result;
    }

    private boolean checkRelateIsEmpty (List<MergeTableProperties> mergeProperties) {
        if (CollectionUtils.isEmpty(mergeProperties)) {
            return true;
        }

        boolean noRelate = false;
        for (MergeTableProperties prop : mergeProperties) {
            if (CollectionUtils.isEmpty(prop.getJoinKeys())) {
                noRelate = true;
                break;
            }
            if (CollectionUtils.isNotEmpty(prop.getChildren())) {
                return checkRelateIsEmpty(prop.getChildren());
            }
        }
        return noRelate;
    }
}
