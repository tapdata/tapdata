package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;


/**
 * @Author: Zed
 * @Date: 2021/12/17
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TransformSchemaAsyncService {

    private TransformSchemaService transformSchemaService;
    private TaskUpdateDagService taskUpdateDagService;


    /**
     * 异步的模型推演，
     *
     */
    @Async
    @Lock(value = "taskId", type = LockType.TRANSFORM_SCHEMA)
    public void transformSchema(DAG dag, UserDetail user, ObjectId taskId) {
        transformSchemaService.transformSchema(dag, user, taskId);
        taskUpdateDagService.updateDag(taskId, dag);
    }

    @Async
    @Lock(value = "taskId", type = LockType.TRANSFORM_SCHEMA)
    public void transformSchema(TaskDto taskDto, UserDetail user, ObjectId taskId) {
        transformSchemaService.transformSchema(taskDto, user);
        taskUpdateDagService.updateDag(taskId, taskDto.getDag());
    }
}
