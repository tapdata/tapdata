package com.tapdata.tm.group.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

/**
 * 分组管理性能监控切面
 * 自动记录关键方法的执行耗时
 *
 */
@Slf4j
@Aspect
@Component
public class GroupPerformanceAspect {

    /**
     * 定义切入点：分组管理服务层的所有公共方法
     */
    @Pointcut("execution(public * com.tapdata.tm.group.service.GroupInfoService.exportGroupInfos*(..)) " +
            "|| execution(public * com.tapdata.tm.group.service.GroupInfoService.batchImportGroup*(..))")
    public void groupServiceMethods() {
    }

    /**
     * 监控服务方法性能
     */
    @Around("groupServiceMethods()")
    public Object monitorServicePerformance(ProceedingJoinPoint joinPoint) throws Throwable {
        String methodName = joinPoint.getSignature().toShortString();
        long startTime = System.currentTimeMillis();

        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - startTime;
            log.info("Method execution time monitor [{}] - {}ms", methodName, duration);
            return result;
        } catch (Throwable throwable) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Method execution failed [{}] - {}ms, error={}", methodName, duration, throwable.getMessage());
            throw throwable;
        }
    }
}
