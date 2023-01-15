package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.cluster.dto.UpdateAgentVersionParam;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.List;


/**
 * 需要区别出来自flowEngin的请求，
 * 如果是来自flowEngin请求，请求头User-Agent:  里会带Java的字符串
 * 如果是来自浏览器用户的点击，User-Agent:带的就是各个浏览器自己的属性。从而去吧
 */
@Aspect
@Component
@Slf4j
public class ClusterStateServiceAop {

    @Autowired
    UserLogService userLogService;

    @Autowired
    WorkerService workerService;

    @Autowired
    ClusterStateService clusterStateService;


    @Pointcut("execution(* com.tapdata.tm.cluster.service.ClusterStateService.updateAgent(..))")
    public void updateAgentPointcut() {

    }


    /**
     * 传入参数是  Map<String, Object> dto, UserDetail userDetail
     *
     * @return
     */
    @After("updateAgentPointcut()")
    public Object afterUpdateAgentPointcut(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return null;
        }
        log.info("点击agent 自动升级");
        Object[] args = joinPoint.getArgs();
        UpdateAgentVersionParam updateAgentVersionParam = (UpdateAgentVersionParam) args[0];
        UserDetail userDetail = (UserDetail) args[1];

        Query query = Query.query(Criteria.where("process_id").is(updateAgentVersionParam.getProcessId()).and("worker_type").is("connector"));
        WorkerDto workerDto = workerService.findOne(query);
        userLogService.addUserLog(Modular.AGENT, Operation.UPDATE, userDetail, workerDto.getTcmInfo().getAgentName());
        return null;
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
