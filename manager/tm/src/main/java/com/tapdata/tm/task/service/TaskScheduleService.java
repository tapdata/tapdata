package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.vo.CalculationEngineVo;

public interface TaskScheduleService {

    void scheduling(TaskDto taskDto, UserDetail user);
    void sendStartMsg(String taskId, String agentId, UserDetail user);
    CalculationEngineVo cloudTaskLimitNum(TaskDto taskDto, UserDetail user);

}
