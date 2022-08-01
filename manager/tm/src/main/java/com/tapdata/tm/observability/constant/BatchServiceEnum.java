package com.tapdata.tm.observability.constant;

import com.tapdata.tm.observability.dto.TaskLogDto;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum BatchServiceEnum {
    TASKCONSOLE(TaskDagCheckLogService.class, "getLogs", TaskLogDto.class, TaskDagCheckLogVo.class);

    private final Class service;
    private final String method;
    private final Class param;
    private final Class result;

    public static BatchServiceEnum getEnumByServiceAndMethod(String service, String method) {
        for (BatchServiceEnum value : BatchServiceEnum.values()) {
            if (value.getService().getSimpleName().equals(service) && value.getMethod().equals(method)) {
                return value;
            }
        }
        return null;
    }
}

