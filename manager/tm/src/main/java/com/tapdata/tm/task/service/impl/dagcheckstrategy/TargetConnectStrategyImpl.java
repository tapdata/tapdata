package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component("targetConnectStrategy")
@Setter(onMethod_ = {@Autowired})
public class TargetConnectStrategyImpl implements DagLogStrategy {
    private DataSourceService dataSourceService;
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_CONNECT_CHECK;
    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        List<TaskDagCheckLog> result = Lists.newArrayList();
        LinkedList<DatabaseNode> targetNode = taskDto.getDag().getTargetNode();

        if (CollectionUtils.isEmpty(targetNode)) {
            return Lists.newArrayList();
        }

        for (DatabaseNode node : targetNode) {
            String connectionId = node.getConnectionId();
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
            if (DataSourceEntity.STATUS_READY.equals(connectionDto.getStatus())) {
                continue;
            }
            Map<String, Object> extParam = Maps.newHashMap();
            extParam.put("taskId", taskDto.getId().toHexString());
            extParam.put("templateEnum", templateEnum);
            extParam.put("userId", userDetail.getUserId());
            extParam.put("type", "target");
            extParam.put("agentId", taskDto.getAgentId());
            extParam.put("taskName", taskDto.getName());
            extParam.put("nodeName", connectionDto.getName());
            extParam.put("alarmCheck", false);
            connectionDto.setExtParam(extParam);

            dataSourceService.sendTestConnection(connectionDto, false, connectionDto.getSubmit(), userDetail);
        }
        return result;
    }
}
