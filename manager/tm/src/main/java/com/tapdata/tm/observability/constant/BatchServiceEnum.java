package com.tapdata.tm.observability.constant;

import com.tapdata.tm.observability.dto.TaskLogDto;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum BatchServiceEnum {
    TASKCONSOLE(TaskDagCheckLogService.class.getName(), "getLogs", "/api/task-console", TaskLogDto.class.getName(), TaskDagCheckLogVo.class.getName());

    private final String service;
    private final String method;
    private final String uri;
    private final String param;
    private final String result;

    public static BatchServiceEnum getEnumByServiceAndMethod(String uri) {
        for (BatchServiceEnum value : BatchServiceEnum.values()) {
            if (value.getUri().equals(uri)) {
                return value;
            }
        }
        return null;
    }
}

