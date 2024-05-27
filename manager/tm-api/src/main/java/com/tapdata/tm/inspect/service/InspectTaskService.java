package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;

import java.util.List;

public interface InspectTaskService {
    InspectDto inspectTaskRun(Where where, InspectDto updateDto, UserDetail user);

    InspectDto inspectTaskStop(String id, InspectDto updateDto, UserDetail user);

    InspectDto inspectTaskError(String id, InspectDto updateDto, UserDetail user);

    InspectDto inspectTaskDone(String id, InspectDto updateDto, UserDetail user);

    InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user);

    String startInspectTask(InspectDto inspectDto, String processId);

    void stopInspectTask(InspectDto inspectDto);

    List<TaskDto> findTaskList(UserDetail user);

    List<DataSourceConnectionDto> findConnectionList(UserDetail user);
}
