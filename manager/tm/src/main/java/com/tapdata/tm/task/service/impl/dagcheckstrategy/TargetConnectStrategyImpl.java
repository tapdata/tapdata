package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("targetConnectStrategy")
public class TargetConnectStrategyImpl implements DagLogStrategy {
    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        return null;
    }
}
