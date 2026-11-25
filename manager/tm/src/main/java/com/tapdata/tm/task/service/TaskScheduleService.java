package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import java.util.Map;
import java.util.Set;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.vo.CalculationEngineVo;

public interface TaskScheduleService {

    void scheduling(TaskDto taskDto, UserDetail user,Boolean isReStart);

    /**
     * Rejects a task whose DAG contains node types unknown to this version before it reaches an engine:
     * writes an error to the task log and moves the task to error.
     *
     * @return true if the task was rejected
     */
    boolean rejectUnknownNodes(TaskDto taskDto, UserDetail user);
    void sendStartMsg(String taskId, String agentId, UserDetail user);
    CalculationEngineVo cloudTaskLimitNum(TaskDto taskDto, UserDetail user, boolean limitNum);
    void validateConnectionAgentPolicy(TaskDto taskDto, UserDetail user);
    void validateConnectionAgentPolicy(TaskDto taskDto, UserDetail user, Map<String, DataSourceConnectionDto> overrides);
    Set<String> connectionAllowedAgents(TaskDto taskDto, UserDetail user, Map<String, DataSourceConnectionDto> overrides);

}
