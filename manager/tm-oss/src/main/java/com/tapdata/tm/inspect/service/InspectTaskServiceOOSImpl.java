package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.webhook.enums.ConstVariable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InspectTaskServiceOOSImpl implements InspectTaskService {
    @Override
    public InspectDto inspectTaskRun(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto inspectTaskStop(String id, InspectDto updateDto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto inspectTaskError(String id, InspectDto updateDto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto inspectTaskDone(String id, InspectDto updateDto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public String startInspectTask(InspectDto inspectDto, String processId) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void stopInspectTask(InspectDto inspectDto) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<TaskDto> findTaskList(UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<DataSourceConnectionDto> findConnectionList(UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }
}
