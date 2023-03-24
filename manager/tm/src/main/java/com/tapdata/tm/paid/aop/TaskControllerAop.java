package com.tapdata.tm.paid.aop;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.paid.dto.CheckPaidPlanRes;
import com.tapdata.tm.paid.service.PaidPlanService;
import com.tapdata.tm.task.controller.TaskController;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/11/22 下午8:13
 */
@Aspect
@Component
@Slf4j
@Profile("dfs")
@Deprecated
public class TaskControllerAop {

    @Autowired
    private PaidPlanService paidPlanService;
    //@Pointcut("execution(* com.tapdata.tm.task.controller.TaskController.save(..))")
    @Deprecated
    public void createTaskPointcut() {

    }

    //@Around("createTaskPointcut()")
    @Deprecated
    public Object beforeCreateTask(ProceedingJoinPoint joinPoint) throws Throwable {

        TaskController taskController = (TaskController) joinPoint.getTarget();
        UserDetail loginUser = taskController.getLoginUser();

        CheckPaidPlanRes result = paidPlanService.checkPaidPlan(loginUser);

        log.debug("Check paid plan for user {}, valid {}", loginUser.getExternalUserId(), result);
        if (result.isValid()) {
            Object[] args = joinPoint.getArgs();
            return joinPoint.proceed(args);
        } else {
            throw new BizException("InvalidPaidPlan",
                    "Invalid paid plan, please check and subscribe paid plan(" +
                            JsonUtil.toJson(result.getMessages()) + ").");
        }
    }

    //@Pointcut("execution(* com.tapdata.tm.task.controller.TaskController.copy(..))")
    @Deprecated
    public void copyTaskPointcut() {

    }

    //@Around("copyTaskPointcut()")
    @Deprecated
    public Object beforeCopyTask(ProceedingJoinPoint joinPoint) throws Throwable {

        TaskController taskController = (TaskController) joinPoint.getTarget();
        UserDetail loginUser = taskController.getLoginUser();

        CheckPaidPlanRes result = paidPlanService.checkPaidPlan(loginUser);

        log.debug("Check paid plan for user {}, valid {}", loginUser.getExternalUserId(), result);
        if (result.isValid()) {
            Object[] args = joinPoint.getArgs();
            return joinPoint.proceed(args);
        } else {
            throw new BizException("InvalidPaidPlan",
                    "Invalid paid plan, please check and subscribe paid plan(" +
                            JsonUtil.toJson(result.getMessages()) + ").");
        }
    }
}
