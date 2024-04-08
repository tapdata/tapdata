package com.tapdata.tm.task.service.utils;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import org.apache.commons.lang3.StringUtils;


public class TaskServiceUtil {
    public void copyAccessNodeInfo(TaskDto source, TaskDto target, UserDetail user, AgentGroupService agentGroupService) {
        if (null == source || null == target) {
            return;
        }
        if (StringUtils.isBlank(target.getAccessNodeType())) {
            target.setAccessNodeType(source.getAccessNodeType());
            target.setAccessNodeProcessId(source.getAccessNodeProcessId());
            target.setAccessNodeProcessIdList(agentGroupService.getProcessNodeListWithGroup(target, user));
        }
    }
}
