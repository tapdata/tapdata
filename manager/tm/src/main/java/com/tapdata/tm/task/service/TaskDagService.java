package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * @author jiuyetx
 * @date 2022/8/30
 */
public interface TaskDagService {
    int calculationDagHash(TaskDto taskDto);
}
