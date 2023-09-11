package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;


@Aspect
@Component
@Slf4j
public class UsersAop {

    @Autowired
    UserLogService userLogService;

    /**
     * 修改通知设置切面
     */
    @Pointcut("execution(* com.tapdata.tm.user.service.UserService.updateUserSetting(..))")
    public void updateUserSettingPointcut() {

    }


    //设置update方法为后置通知
    @AfterReturning(value = "updateUserSettingPointcut()", returning = "result")
    public void afterSave(JoinPoint joinPoint,Object result) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return;
        }

        log.info("执行了 修改通知设置 操作");
        UserDto userDto = (UserDto) result;
        Object[] args = joinPoint.getArgs();
        Locale locale=(Locale) args[3];
        if (null != userDto) {
            userLogService.addUserLog(Modular.USER_NOTIFICATION, Operation.UPDATE, userDto.getId().toString(), userDto.getId().toString(), MessageUtil.getLogMsg(locale,"NotificationSettings"));
        }
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
