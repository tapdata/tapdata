package com.tapdata.tm.task.service;

import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.service.StateMachineService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author: Zed
 * @Date: 2021/12/18
 * @Description:
 */
@Service
@Slf4j
public class TaskStartService {

    @Autowired
    private SubTaskService subTaskService;

    @Autowired
    private CustomerJobLogsService customerJobLogsService;

    @Async
    public void start0(TaskDto taskDto, UserDetail user) {
        //DAG dag = taskDto.getDag();
        //校验数据源模型是否存在，不存在则需要执行模型推演。

        //日志挖掘任务不需要模型推演
//        if(!dag.isLogCollectorDag()) {
//            Map<String, List<Message>> schemaErrorMap = transformSchemaService.transformSchemaSync(dag, user, taskDto.getId());
//            if (!schemaErrorMap.isEmpty()) {
//                throw new BizException("Task.ListWarnMessage", schemaErrorMap);
//            }
//        }

        log.debug("check task success, task name = {}", taskDto.getName());
        CustomerJobLog customerJobLog = new CustomerJobLog(taskDto.getId().toString(),taskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogsService.startDataFlow(customerJobLog, user);

        //共享挖掘
        subTaskService.logCollector(user, taskDto);

        //打点任务
        //subTaskService.startConnHeartbeat(user, taskDto);


        //启动所有的子任务
        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(taskDto.getId());
        for (SubTaskDto subTaskDto : subTaskDtos) {

            try {
                //stateMachineService.executeAboutSubTask(subTaskDto, DataFlowEvent.START, user);
                subTaskService.start(subTaskDto.getId(), user, "00");
            } catch (BizException e) {
                //不能一个子任务报错，其他的子任务不能启动了
                log.warn("start sub task error, sub task name = {}, e = {}", subTaskDto.getName(), e.getErrorCode());
            }
        }
        log.info("start task success, task name = {}", taskDto.getName());
    }

}
