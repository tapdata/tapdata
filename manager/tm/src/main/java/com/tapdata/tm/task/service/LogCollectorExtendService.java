package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.task.vo.LogCollectorRelateTaskVo;

public interface LogCollectorExtendService {
    Page<LogCollectorRelateTaskVo> getRelationTask(String taskId, String type, Integer page, Integer size);
}
