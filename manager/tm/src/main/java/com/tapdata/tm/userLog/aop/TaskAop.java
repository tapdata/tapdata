package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Aspect
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskAop {

    UserLogService userLogService;
    TaskService taskService;

    @Pointcut("execution(* com.tapdata.tm.task.service.TaskService.start(..)) || execution(* com.tapdata.tm.task.service.TaskService.batchStart(..))")
    public void startPointcut() {}

    @Pointcut("execution(* com.tapdata.tm.task.service.TaskService.pause(..)) || execution(* com.tapdata.tm.task.service.TaskService.batchStop(..))")
    public void stoppedPointcut() {}

    @After("stoppedPointcut()")
    public Object afterStoppedPointcut(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();

        UserDetail userDetail = (UserDetail) args[1];
        if (args[0] instanceof ObjectId) {
            ObjectId id = (ObjectId) args[0];
            boolean force = (boolean)args[2];

            //查询任务是否存在
            TaskDto taskDto = taskService.checkExistById(id, userDetail);

            Operation operation = force ? Operation.FORCE_STOP : Operation.STOP;
            if (null != taskDto) {
                userLogService.addUserLog("sync".equals(taskDto.getSyncType()) ? Modular.SYNC : Modular.MIGRATION, operation, userDetail, taskDto.getId().toString(), taskDto.getName());
            }
        } else if (args[0] instanceof List<?>){
            List<?> list = (List<?>) args[0];

            List<ObjectId> ObjectIds = list.stream().map(s -> (ObjectId) s).collect(Collectors.toList());

            List<TaskEntity> taskList = taskService.findByIds(ObjectIds);

            Operation operation = Operation.STOP;
            if (CollectionUtils.isNotEmpty(taskList)) {
                taskList.forEach(taskDto ->
                    userLogService.addUserLog("sync".equals(taskDto.getSyncType()) ? Modular.SYNC : Modular.MIGRATION, operation, userDetail, taskDto.getId().toString(), taskDto.getName())
                );
            }
        }

        return null;
    }

    @After("startPointcut()")
    public Object afterStartPointcut(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();

        UserDetail userDetail = (UserDetail) args[1];
        Object arg = args[0];
        if (arg instanceof ObjectId) {
            ObjectId id = (ObjectId) arg;

            //查询任务是否存在
            TaskDto taskDto = taskService.checkExistById(id, userDetail);
            if (null != taskDto) {
                userLogService.addUserLog("sync".equals(taskDto.getSyncType()) ? Modular.SYNC : Modular.MIGRATION, Operation.START, userDetail, taskDto.getId().toString(), taskDto.getName());
            }

        } else if (arg instanceof TaskDto) {
            TaskDto taskDto = (TaskDto) arg;
            userLogService.addUserLog("sync".equals(taskDto.getSyncType()) ? Modular.SYNC : Modular.MIGRATION, Operation.START, userDetail, taskDto.getId().toString(), taskDto.getName());

        }else if (arg instanceof List<?>){
            List<?> list = (List<?>) arg;

            List<ObjectId> ObjectIds = list.stream().map(s -> (ObjectId) s).collect(Collectors.toList());

            List<TaskEntity> taskList = taskService.findByIds(ObjectIds);

            if (CollectionUtils.isNotEmpty(taskList)) {
                taskList.forEach(taskDto ->
                        userLogService.addUserLog("sync".equals(taskDto.getSyncType()) ? Modular.SYNC : Modular.MIGRATION, Operation.START, userDetail, taskDto.getId().toString(), taskDto.getName())
                );
            }
        }
        return null;
    }
}
