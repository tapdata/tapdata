//package com.tapdata.tm.userLog.aop;
//
//import com.tapdata.manager.common.utils.JsonUtil;
//import com.tapdata.tm.Settings.service.SettingsService;
//import com.tapdata.tm.commons.task.dto.SubTaskDto;
//import com.tapdata.tm.config.security.UserDetail;
//import com.tapdata.tm.inspect.constant.InspectResultEnum;
//import com.tapdata.tm.inspect.constant.InspectStatusEnum;
//import com.tapdata.tm.inspect.dto.InspectDto;
//import com.tapdata.tm.inspect.service.InspectService;
//import com.tapdata.tm.message.constant.Level;
//import com.tapdata.tm.message.constant.MsgTypeEnum;
//import com.tapdata.tm.message.service.MessageService;
//import com.tapdata.tm.task.service.SubTaskService;
//import com.tapdata.tm.user.entity.Notification;
//import com.tapdata.tm.user.service.UserService;
//import com.tapdata.tm.userLog.constant.Modular;
//import com.tapdata.tm.userLog.constant.Operation;
//import com.tapdata.tm.userLog.service.UserLogService;
//import com.tapdata.tm.utils.MongoUtils;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.aspectj.lang.JoinPoint;
//import org.aspectj.lang.annotation.After;
//import org.aspectj.lang.annotation.AfterReturning;
//import org.aspectj.lang.annotation.Aspect;
//import org.aspectj.lang.annotation.Pointcut;
//import org.bson.types.ObjectId;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//import org.springframework.web.context.request.RequestContextHolder;
//import org.springframework.web.context.request.ServletRequestAttributes;
//
//import javax.servlet.http.HttpServletRequest;
//
//
///**
// * 需要区别出来自flowEngin的请求，
// * 如果是来自flowEngin请求，请求头User-Agent:  里会带Java的字符串
// * 如果是来自浏览器用户的点击，User-Agent:带的就是各个浏览器自己的属性。从而去吧
// */
//@Aspect
////@Component
//@Slf4j
//public class SubTaskAop {
//
//    @Autowired
//    UserLogService userLogService;
//
//    @Autowired
//    MessageService messageService;
//
//    @Autowired
//    UserService userService;
//
//    @Autowired
//    SettingsService settingsService;
//
//    @Autowired
//    SubTaskService subTaskService;
//
//
//  /*  @Pointcut("execution(* com.tapdata.tm.task.service.InspectService.save(..))")
//    public void savePointCut() {
//
//    }
//*/
//
//  /*  @Pointcut("execution(* com.tapdata.tm.task.service.SubTaskService.delete(..))")
//    public void deletePointCut() {
//
//    }*/
//
//    @Pointcut("execution(* com.tapdata.tm.task.service.SubTaskService.running(..))")
//    public void runningPointcut() {
//
//    }
//
//
//    @Pointcut("execution(* com.tapdata.tm.task.service.SubTaskService.stopped(..))")
//    public void stoppedPointcut() {
//
//    }
//
//    @Pointcut("execution(* com.tapdata.tm.task.service.SubTaskService.runError(..))")
//    public void runErrorPointcut() {
//
//    }
//
//    @Pointcut("execution(* com.tapdata.tm.task.service.SubTaskService.complete(..))")
//    public void completePointcut() {
//
//    }
//    /**
//     * 启动任务拦截
//     *
//     * @return
//     */
//    @After("runningPointcut()")
//    public Object afterDeletePointcut(JoinPoint joinPoint) {
//       /* if (!shouldRecord()) {
//            log.info("不是来自用户操作");
//            return null;
//        }*/
//
//        Object[] args = joinPoint.getArgs();
//        ObjectId id = (ObjectId) args[0];
//        UserDetail userDetail = (UserDetail) args[1];
//        SubTaskDto subTaskDto = subTaskService.findById(id);
//        Notification notification = userDetail.getNotification();
//        if (null == notification) {
//            //企业版，要add message
//            addMessage(subTaskDto, userDetail);
//
//        }
//        return null;
//    }
//
//    /**
//     * 启动任务拦截
//     *
//     * @return
//     */
//    @After("stoppedPointcut()")
//    public Object afterStoppedPointcut(JoinPoint joinPoint) {
//        Object[] args = joinPoint.getArgs();
//        ObjectId id = (ObjectId) args[0];
//        UserDetail userDetail = (UserDetail) args[1];
//        SubTaskDto subTaskDto = subTaskService.findById(id);
//        if (null != subTaskDto) {
//            userLogService.addUserLog(Modular.SYNC, Operation.STOP, userDetail, subTaskDto.getId().toString(), subTaskDto.getName());
//        }
//        Notification notification = userDetail.getNotification();
//        if (null == notification) {
//            //企业版，要add message
//            addMessage(subTaskDto, userDetail);
//        }
//        return null;
//    }
//
//
//    /**
//     * 启动任务拦截
//     *
//     * @return
//     */
//    @After("runErrorPointcut()")
//    public Object afterRunErrorPointcut(JoinPoint joinPoint) {
//        Object[] args = joinPoint.getArgs();
//        ObjectId id = (ObjectId) args[0];
//        UserDetail userDetail = (UserDetail) args[1];
//        SubTaskDto subTaskDto = subTaskService.findById(id);
//
//        Notification notification = userDetail.getNotification();
//        if (null == notification) {
//            //企业版，要add message
//            addMessage(subTaskDto, userDetail);
//        }
//        return null;
//    }
//
//    @After("completePointcut()")
//    public Object afterCompletePointcut(JoinPoint joinPoint) {
//        Object[] args = joinPoint.getArgs();
//        ObjectId id = (ObjectId) args[0];
//        UserDetail userDetail = (UserDetail) args[1];
//        SubTaskDto subTaskDto = subTaskService.findById(id);
//
//        Notification notification = userDetail.getNotification();
//        if (null == notification) {
//            //企业版，要add message
//            addMessage(subTaskDto, userDetail);
//        }
//        return null;
//    }
//
//
//    private void addMessage(SubTaskDto subTaskDto, UserDetail userDetail) {
//        log.info("同步任务启动 完成 subTaskDto:{}", JsonUtil.toJson(subTaskDto));
//        String status = subTaskDto.getStatus();
//        String name = subTaskDto.getName();
//        String sourceId = subTaskDto.getId().toString();
//
//        if (SubTaskDto.STATUS_RUNNING.equals(status)) {
//            log.info("同步任务 启动 subTaskDto:{}");
//            messageService.addSync(name, sourceId, MsgTypeEnum.STARTED, null, Level.INFO, userDetail);
//        } else if (SubTaskDto.STATUS_ERROR.equals(status)) {
//            log.info("同步任务 出错 subTaskDto:{}");
//            messageService.addSync(name, sourceId, MsgTypeEnum.STOPPED_BY_ERROR, null, Level.ERROR, userDetail);
//        } else if (SubTaskDto.STATUS_STOP.equals(status)) {
//            log.info("同步任务 暂停 subTaskDto:{}");
//            messageService.addSync(name, sourceId, MsgTypeEnum.PAUSED, null, Level.INFO, userDetail);
//        }   else if (SubTaskDto.STATUS_COMPLETE.equals(status)) {
//            log.info("同步任务 完成 subTaskDto:{}");
//            messageService.addSync(name, sourceId, MsgTypeEnum.COMPLETED, null, Level.INFO, userDetail);
//        }
//    }
//
//
//    private Boolean shouldRecord() {
//        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
//        HttpServletRequest request = attributes.getRequest();
//        if (null == request) {
//            return false;
//        }
//        String userAgent = request.getHeader("User-Agent");
//        if (StringUtils.isNotEmpty(userAgent) && (userAgent.contains("Java") || userAgent.contains("java") || userAgent.contains("nodejs"))) {
//            return false;
//        }
//        return true;
//    }
//
//}
