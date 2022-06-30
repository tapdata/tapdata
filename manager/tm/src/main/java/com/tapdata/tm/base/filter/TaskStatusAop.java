//package com.tapdata.tm.base.filter;
//
//import com.tapdata.tm.base.dto.Page;
//import com.tapdata.tm.commons.task.dto.TaskDto;
//import com.tapdata.tm.task.entity.TaskEntity;
//import com.tapdata.tm.task.service.TaskService;
//import org.aspectj.lang.JoinPoint;
//import org.aspectj.lang.annotation.AfterReturning;
//import org.aspectj.lang.annotation.Aspect;
//import org.aspectj.lang.annotation.Pointcut;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import java.util.List;
//
///**
// * @Author: Zed
// * @Date: 2021/11/13
// * @Description:
// */
//@Aspect
//@Component
//public class TaskStatusAop {
//
//    @Autowired
//    private TaskService taskService;
//
//    @Pointcut("execution(public * com.tapdata.tm.task.service.TaskService.*Proxy(..))")
//    private void findMethod(){}
//
//    @AfterReturning(returning = "result", pointcut = "findMethod()")
//    public void afterReturn(JoinPoint joinPoint, Object result) {
//        if (result instanceof List) {
//            List results = (List) result;
//            for (Object o : results) {
//                taskService.flushStatus(o);
//            }
//        } else if (result instanceof TaskDto) {
//            taskService.flushStatus(result);
//        } else if (result instanceof TaskEntity) {
//            taskService.flushStatus(result);
//        } else if (result instanceof Page) {
//            List<?> items = ((Page<?>) result).getItems();
//            for (Object item : items) {
//                taskService.flushStatus(item);
//            }
//        }
//    }
//
//}
