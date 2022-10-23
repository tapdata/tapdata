package com.tapdata.tm.task.service;

import com.tapdata.tm.task.vo.RelationTaskInfoVo;
import com.tapdata.tm.task.vo.RelationTaskRequest;

import java.util.List;

public interface TaskConsoleService {
    List<RelationTaskInfoVo> getRelationTasks(RelationTaskRequest request);
}
