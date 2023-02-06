package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
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
import java.util.Objects;

@Component("targetConnectStrategy")
@Setter(onMethod_ = {@Autowired})
public class TargetConnectStrategyImpl implements DagLogStrategy {
    private DataSourceService dataSourceService;
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_CONNECT_CHECK;
    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {

        DAG dag = taskDto.getDag();
        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        List<TaskDagCheckLog> result = Lists.newArrayList();
        for (Node node : dag.getTargets()) {
            String connectionId = node instanceof DatabaseNode ? ((DatabaseNode) node).getConnectionId() : ((TableNode) node).getConnectionId();
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
