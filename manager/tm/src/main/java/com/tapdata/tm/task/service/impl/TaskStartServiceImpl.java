package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TaskStartService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * @Author: Zed
 * @Date: 2021/12/18
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskStartServiceImpl implements TaskStartService {
    private TaskDagCheckLogService taskDagCheckLogService;
    private TaskService taskService;

    @Async
    public void start0(TaskDto taskDto, UserDetail user) {
        //DAG dag = taskDto.getDag();
        //校验数据源模型是否存在，不存在则需要执行模型推演。

        //日志挖掘任务不需要模型推演
////        if(!dag.isLogCollectorDag()) {
////            Map<String, List<Message>> schemaErrorMap = transformSchemaService.transformSchemaSync(dag, user, taskDto.getId());
////            if (!schemaErrorMap.isEmpty()) {
////                throw new BizException("Task.ListWarnMessage", schemaErrorMap);
////            }
////        }
//
//        log.debug("check task success, task name = {}", taskDto.getName());
//        CustomerJobLog customerJobLog = new CustomerJobLog(taskDto.getId().toString(),taskDto.getName());
//        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
//        customerJobLogsService.startDataFlow(customerJobLog, user);
//
//        //共享挖掘
//        taskService.logCollector(user, taskDto);
//
//        //打点任务
//        //subTaskService.startConnHeartbeat(user, taskDto);
//
//
//        //启动所有的子任务
//        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(taskDto.getId());
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//
//            try {
//                //stateMachineService.executeAboutSubTask(subTaskDto, DataFlowEvent.START, user);
//                subTaskService.start(subTaskDto.getId(), user, "00");
//            } catch (BizException e) {
//                //不能一个子任务报错，其他的子任务不能启动了
//                log.warn("start sub task error, sub task name = {}, e = {}", subTaskDto.getName(), e.getErrorCode());
//            }
//        }
//        log.info("start task success, task name = {}", taskDto.getName());
    }

    @Override
    public boolean taskStartCheckLog(TaskDto taskDto, UserDetail userDetail) {
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            return false;
        }

        taskDagCheckLogService.removeAllByTaskId(taskDto.getId().toHexString());

        boolean saveNoPass = false;
        List<TaskDagCheckLog> saveLogs = taskDagCheckLogService.dagCheck(taskDto, userDetail, true);
        if (CollectionUtils.isNotEmpty(saveLogs)) {
            Optional<TaskDagCheckLog> any = saveLogs.stream().filter(log -> Level.ERROR.equals(log.getGrade())).findAny();
            if (any.isPresent()) {
                saveNoPass = true;
                //taskService.updateStatus(taskDto.getId(), TaskDto.STATUS_EDIT);
            }
        }

        boolean startNoPass = false;
        List<TaskDagCheckLog> startLogs = taskDagCheckLogService.dagCheck(taskDto, userDetail, false);
        if (CollectionUtils.isNotEmpty(startLogs)) {
            Optional<TaskDagCheckLog> any = startLogs.stream().filter(log -> Level.ERROR.equals(log.getGrade())).findAny();
            if (any.isPresent()) {
                startNoPass = true;
                if (!saveNoPass) {
                    //taskService.updateStatus(taskDto.getId(), TaskDto.STATUS_EDIT);
                }
            }
        }

        return saveNoPass & startNoPass;
    }

}
