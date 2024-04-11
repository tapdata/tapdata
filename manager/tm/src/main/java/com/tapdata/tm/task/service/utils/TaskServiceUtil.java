package com.tapdata.tm.task.service.utils;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.lang3.StringUtils;


public class TaskServiceUtil {
    private TaskServiceUtil() {
    }

    public static void copyAccessNodeInfo(TaskDto source, TaskDto target) {
        if (null == source || null == target) {
            return;
        }
        if (StringUtils.isBlank(target.getAccessNodeType())) {
            target.setAccessNodeType(source.getAccessNodeType());
            target.setAccessNodeProcessId(source.getAccessNodeProcessId());
            target.setAccessNodeProcessIdList(source.getAccessNodeProcessIdList());
        }
    }
}
