package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;


/**
 * 需要区别出来自flowEngin的请求，
 * 如果是来自flowEngin请求，请求头User-Agent:  里会带Java的字符串
 * 如果是来自浏览器用户的点击，User-Agent:带的就是各个浏览器自己的属性。从而去吧
 */
@Aspect
@Component
@Slf4j
public class InspectAop {

    @Autowired
    UserLogService userLogService;

    @Autowired
    MessageService messageService;

    @Autowired
    UserService userService;

    @Autowired
    SettingsService settingsService;

    @Autowired
    InspectService inspectService;


    @Pointcut("execution(* com.tapdata.tm.inspect.service.InspectService.save(..))")
    public void savePointCut() {

    }


  /*  @Pointcut("execution(* com.tapdata.tm.inspect.service.InspectService.delete(..))")
    public void deletePointCut() {

    }*/

  /*  @Pointcut("execution(* com.tapdata.tm.inspect.service.InspectService.updateInspectByWhere(..))")
    public void updateInspectByWherePointcut() {

    }*/

    @Pointcut("execution(* com.tapdata.tm.base.service.BaseService.upsertByWhere(..))  &&   target(com.tapdata.tm.inspect.service.InspectService)")
    public void updateByWherePointcut() {

    }

    /**
     * 编辑数据校验切面
     */
    @Pointcut("execution(* com.tapdata.tm.inspect.service.InspectService.updateById(..))")
    public void updateByIdPointcut() {

    }


    //设置update方法为后置通知
    @AfterReturning(value = "savePointCut()", returning = "result")
    public void afterSave(Object result) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return;
        }

        log.info("执行了 新增数据校验 操作");
        InspectDto inspectDto = (InspectDto) result;
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(inspectDto.getUserId()));
        userLogService.addUserLog(Modular.INSPECT, Operation.CREATE, userDetail, inspectDto.getId().toString(), inspectDto.getName());

    }


    /**
     * 删除校验拦截
     *
     * @return
     */
   /* @After("deletePointCut()")
    public Object afterDeletePointcut(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return null;
        }

        Object[] args = joinPoint.getArgs();
        String id = (String) args[0];
        UserDetail userDetail = (UserDetail) args[1];
        InspectDto inspectDto = inspectService.findById(new ObjectId(id));

        userLogService.addUserLog(Modular.INSPECT, Operation.DELETE, userDetail, inspectDto.getId().toString(), inspectDto.getName());

        Notification notification = userDetail.getNotification();
        if (null == notification) {
            //企业版，要add message
            addMessage(id, userDetail);
        }
        return null;
    }
*/

    /**
     * 执行校验拦截
     *
     * @param result
     * @return
     */
/*    @AfterReturning(value = "updateInspectByWherePointcut()", returning = "result")
    public Object afterUpdateInspectByWherePointcut(Object result) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return null;
        }
        try {
            InspectDto inspectDto = (InspectDto) result;
            if (null != inspectDto) {
                userLogService.addUserLog(Modular.INSPECT, Operation.START, inspectDto.getUserId(), inspectDto.getId().toString(), inspectDto.getName());
            }

        } catch (Exception e) {
            log.error("保存 执行校验 操作日志异常", e);
        }
        return null;
    }*/


    /**
     * 编辑数据校验拦截
     *
     * @param result
     */
    @AfterReturning(value = "updateByIdPointcut()", returning = "result")
    public void afterUpdateById(Object result) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return;
        }

        log.info("执行了 编辑数据校验 操作");
        InspectDto inspectDto = (InspectDto) result;
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(inspectDto.getUserId()));
        userLogService.addUserLog(Modular.INSPECT, Operation.UPDATE, userDetail, inspectDto.getId().toString(), inspectDto.getName());

    }


    /**
     * 企业版 更新校验结果拦截
     *
     * @return
     */
  /*  @After("updateByWherePointcut()")
    public Object afterUpdateByWherePointcut(JoinPoint joinPoint) {
        log.info("企业版 更新校验结果拦截");
        try {
            Object[] args = joinPoint.getArgs();
            Where where = (Where) args[0];
            ObjectId id = (ObjectId) where.get("id");
            if (null != id) {
                log.error("传参有误，id为空");
                InspectDto inspectDto = (InspectDto) args[1];
                UserDetail userDetail = (UserDetail) args[2];

                if (null != inspectDto && null != userDetail) {
                    addMessage(id.toString(), userDetail);
                }
            } else {
                log.error(" 传参有误，id为空");
            }
        } catch (Exception e) {
            log.error("更新校验结果 ，更新通知异常", e);
        }
        return null;
    }*/

    private void addMessage(String id, UserDetail userDetail) {
        log.info("inspect addMessage id:{} ", id);
        InspectDto inspectDto = inspectService.findById(MongoUtils.toObjectId(id));
        if (null == inspectDto) {
            return;
        }
        String status = inspectDto.getStatus();
        String result = inspectDto.getResult();
        String name = inspectDto.getName();
        String sourceId = inspectDto.getId().toString();


        if (InspectStatusEnum.ERROR.getValue().equals(status)) {
            log.info("校验 出错 inspect:{}");
            messageService.addInspect(name, sourceId, MsgTypeEnum.INSPECT_ERROR, Level.ERROR, userDetail);
        } else if (InspectStatusEnum.DONE.getValue().equals(status)) {
           /* log.info("校验 完成 inspect:{}");
            if (InspectResultEnum.FAILED.equals(result)) {
                messageService.addInspect(name, sourceId, MsgTypeEnum.INSPECT_VALUE, Level.ERROR, userDetail);
            }*/
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
