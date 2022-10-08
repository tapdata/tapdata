package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
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

@Component("sourceConnectStrategy")
@Setter(onMethod_ = {@Autowired})
public class SourceConnectStrategyImpl implements DagLogStrategy {

    private DataSourceService dataSourceService;
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.SOURCE_CONNECT_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        List<TaskDagCheckLog> result = Lists.newArrayList();
        LinkedList<DatabaseNode> sourceNode = taskDto.getDag().getSourceNode();

        if (CollectionUtils.isEmpty(sourceNode)) {
            return Lists.newArrayList();
        }

        for (DatabaseNode node : sourceNode) {
            String connectionId = node.getConnectionId();
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
            connectionDto.setExtParam(
                    ImmutableMap.of("taskId", taskDto.getId().toHexString(),
                            "templateEnum", templateEnum,
                            "userId", userDetail.getUserId(),
                            "type", "source",
                            "agentId", taskDto.getAgentId(),
                            "taskName", taskDto.getName(),
                            "nodeName", connectionDto.getName(),
                            "alarmCheck", false
                    )
            );

            dataSourceService.sendTestConnection(connectionDto, false, connectionDto.getSubmit(), userDetail);
        }


        return result;
    }
}
