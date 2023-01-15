package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * 拦截任务方法，以便写入操作日志
 * // 通知
 * { label: '已读全部通知', value: 'message_readAll', desc: '设置全部通知为已读' },
 * { label: '删除全部通知', value: 'message_deleteAll', desc: '删除了全部通知' },
 * { label: '标记通知为已读', value: 'message_read', desc: '将选中的通知全部标记为已读' },
 * { label: '删除通知', value: 'message_delete', desc: '将选中的通知全部删除' },
 * { label: '修改通知设置', value: 'userNotification_update', desc: '修改了系统通知设置' }
 */
@Aspect
@Component
@Slf4j
@Deprecated
public class MessageAop {

    @Autowired
    MessageService messageService;

    @Autowired
    UserLogService userLogService;

    @Pointcut("execution(* com.tapdata.tm.message.service.MessageService.read(..))")
    public void readPointcut() {

    }


    @Pointcut("execution(* com.tapdata.tm.message.service.MessageService.delete(..))")
    public void deletePointcut() {

    }


    @Pointcut("execution(* com.tapdata.tm.message.service.MessageService.deleteByUserId(..))")
    public void deleteByUserIdPointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.message.service.MessageService.readAll(..))")
    public void readAllPointcut() {

    }


   /* @Around(value = "readPointcut()")
    public Object afterReadReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        Object ret = null;
        Object[] args = joinPoint.getArgs();

        List<String> ids = (List<String>) args[0];
        UserDetail userDetail = (UserDetail) args[1];

        ret = joinPoint.proceed(args);
        if (CollectionUtils.isNotEmpty(ids)) {
            ids.forEach(id -> {
                userLogService.addUserLog(Modular.MESSAGE, Operation.READ, userDetail, id, "已读通知");
            });
        }
        return ret;
    }
*/
    /**
     * 拦截已读全部通知
     *
     * @param joinPoint
     * @return
     * @throws Throwable
     */
   /* @Around(value = "readAllPointcut()")
    public Object afterReadAll(ProceedingJoinPoint joinPoint) throws Throwable {
        Object ret = null;
        Object[] args = joinPoint.getArgs();

        UserDetail userDetail = (UserDetail) args[0];

        ret = joinPoint.proceed(args);
        userLogService.addUserLog(Modular.MESSAGE, Operation.READ_ALL, userDetail.getUserId(), null, "全部通知");
        return ret;
    }*/


    /**
     * 删除单挑消息后置通知
     *
     * @param joinPoint
     * @return
     * @throws Throwable
     */
    @Around("deletePointcut()")
    public Object afterDeleteReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        Object ret = null;
        Object[] args = joinPoint.getArgs();
        UserDetail userDetail = (UserDetail) args[1];
        List<String> ids = (List<String>) args[0];
        ret = joinPoint.proceed(args);
        if (CollectionUtils.isNotEmpty(ids)) {
            ids.forEach(id -> {
                userLogService.addUserLog(Modular.MESSAGE, Operation.DELETE, userDetail, id, "删除通知");
            });
        }

        return ret;
    }

    /**
     * 删除用户所有通知
     *
     * @param joinPoint
     * @return
     * @throws Throwable
     */
    @Around("deleteByUserIdPointcut()")
    public Object deleteByUserId(ProceedingJoinPoint joinPoint) throws Throwable {
        Object ret = null;
        Object[] args = joinPoint.getArgs();
        UserDetail userDetail = (UserDetail) args[0];
        ret = joinPoint.proceed(args);
        userLogService.addUserLog(Modular.MESSAGE, Operation.DELETE_ALL, userDetail.getUserId(), null, "全部通知");

        return ret;
    }


    private Boolean shouldRecord() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();
        if (null == request) {
            return false;
        }
        String userAgent = request.getHeader("User-Agent");
        if (StringUtils.isNotEmpty(userAgent) && (userAgent.contains("Java") || userAgent.contains("java") || userAgent.contains("nodejs"))) {
            return false;
        }
        return true;
    }

}
