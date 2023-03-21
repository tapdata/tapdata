package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Component("targetConnectStrategy")
@Setter(onMethod_ = {@Autowired})
public class TargetConnectStrategyImpl implements DagLogStrategy {
    private DataSourceService dataSourceService;
    private TaskDagCheckLogService taskDagCheckLogService;
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_CONNECT_CHECK;
    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        DAG dag = taskDto.getDag();
        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        String taskId = taskDto.getId().toHexString();
        String userId = userDetail.getUserId();
        Level grade;
        String template;

        List<TaskDagCheckLog> result = Lists.newArrayList();
        for (Node node : dag.getTargets()) {
            String connectionId = ((DataParentNode) node).getConnectionId();
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
            if (Objects.isNull(connectionDto)) {
                continue;
            }

            if (DataSourceEntity.STATUS_READY.equals(connectionDto.getStatus())) {
                grade = Level.INFO;
                template = MessageUtil.getDagCheckMsg(locale, "TARGET_CONNECT_INFO");
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, node.getId(), userId, grade, templateEnum, template, connectionDto.getName());
                result.add(log);
            } else {
                grade = Level.ERROR;
                template = MessageUtil.getDagCheckMsg(locale, "TARGET_CONNECT_ERROR");
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, node.getId(), userId, grade, templateEnum, template, connectionDto.getName(), connectionDto.getAlarmInfo());
                result.add(log);
            }

        }
        return result;
    }
}
