package com.tapdata.tm.skiperrortable.aop;

import com.tapdata.tm.skiperrortable.service.ITaskSkipErrorTableService;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/9 14:09 Create
 */
@Aspect
@Component
@Slf4j
public class TaskSkipErrorTableAop {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSkipErrorTableAop.class);

    private final ITaskSkipErrorTableService service;

    @Autowired
    public TaskSkipErrorTableAop(ITaskSkipErrorTableService service) {
        this.service = service;
    }

    @Pointcut("execution(* com.tapdata.tm.task.service.TaskResetLogService.clearLogByTaskId(String))")
    public void renewPointCut() {
    }

    @After("renewPointCut()")
    public Object afterUpdateByIdPointcut(JoinPoint joinPoint) {
        Object taskIdObj = joinPoint.getArgs()[0];
        if (taskIdObj instanceof String taskId) {
            long deleteTotals = service.deleteByTaskId(taskId, null);
            if (deleteTotals > 0) {
                LOGGER.info("delete skip error table status {} records", deleteTotals);
            }
        }
        return joinPoint.getTarget();
    }

}
