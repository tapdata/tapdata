package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.script.py.MigratePyProcessNode;
import com.tapdata.tm.commons.dag.process.script.py.PyProcessNode;
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

@Component("jsSettingStrategy")
public class JsSettingStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.JS_NODE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        String taskId = taskDto.getId().toHexString();

        Date now = new Date();
        String userId = userDetail.getUserId();
        List<TaskDagCheckLog> result = Lists.newArrayList();
        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        dag.getNodes().stream()
                .filter(node -> node instanceof JsProcessorNode || node instanceof MigrateJsProcessorNode || node instanceof PyProcessNode || node instanceof MigratePyProcessNode)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();
                    String nodeName = NodeEnum.valueOf(node.getType()).getNodeName();

                    if (StringUtils.isEmpty(name)) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "JS_EDIT_NAME_EMPTY", DagCheckLogs.nodeName(node, nodeName)));
                    }

                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.INFO, templateEnum, locale, "JS_EDIT_PASS", DagCheckLogs.nodeName(node, nodeName), name));
                    }
                });

        return result;
    }
}
