package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.cluster.dto.UpdateAgentVersionParam;
import com.tapdata.tm.clusterOperation.constant.ClusterOperationTypeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.AuditEventType;
import com.tapdata.tm.userLog.constant.AuditOutcome;
import com.tapdata.tm.userLog.param.AuditLogParam;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;


/**
 * 需要区别出来自flowEngin的请求，
 * 如果是来自flowEngin请求，请求头User-Agent:  里会带Java的字符串
 * 如果是来自浏览器用户的点击，User-Agent:带的就是各个浏览器自己的属性。从而去吧
 */
@Aspect
@Component
public class ClusterStateServiceAop {

    @Autowired
    UserLogService userLogService;

    @Autowired
    WorkerService workerService;


    @Pointcut("execution(* com.tapdata.tm.cluster.service.ClusterStateService.updateAgent(..))")
    public void updateAgentPointcut() {

    }


    /**
     * 传入参数是  Map<String, Object> dto, UserDetail userDetail
     *
     * @return
     */
    @AfterReturning("updateAgentPointcut()")
    public void afterUpdateAgentPointcut(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            return;
        }
        Object[] args = joinPoint.getArgs();
        UpdateAgentVersionParam updateAgentVersionParam = (UpdateAgentVersionParam) args[0];
        UserDetail userDetail = (UserDetail) args[1];
        recordLifecycleAudit(updateAgentVersionParam, userDetail, AuditOutcome.SUCCESS, null);
    }

    @AfterThrowing("updateAgentPointcut()")
    public void afterUpdateAgentFailure(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            return;
        }
        Object[] args = joinPoint.getArgs();
        recordLifecycleAudit((UpdateAgentVersionParam) args[0], (UserDetail) args[1],
                AuditOutcome.FAILURE, "service_operation_failed");
    }

    private void recordLifecycleAudit(UpdateAgentVersionParam updateParam, UserDetail operator,
                                      AuditOutcome outcome, String failureReason) {
        Query query = Query.query(Criteria.where("process_id").is(updateParam.getProcessId())
                .and("worker_type").is("connector"));
        WorkerDto worker = workerService.findOne(query);
        String action = ClusterOperationTypeEnum.restart.name().equals(updateParam.getOp()) ? "restart"
                : ClusterOperationTypeEnum.start.name().equals(updateParam.getOp()) ? "start" : "update";
        AuditLogParam param = new AuditLogParam();
        param.setEventType(AuditEventType.SERVICE_LIFECYCLE);
        param.setOutcome(outcome);
        param.setUserId(operator == null ? "SYSTEM" : operator.getUserId());
        param.setUsername(operator == null ? "SYSTEM" : operator.getUsername());
        param.setAction(action);
        param.setObjectName(resolveServiceName(worker, updateParam.getProcessId()));
        param.setServiceNode(resolveServiceNode(worker, updateParam.getProcessId()));
        param.setFailureReason(failureReason);
        userLogService.addAuditLog(param);
    }

    private String resolveServiceName(WorkerDto worker, String processId) {
        return worker != null && worker.getTcmInfo() != null
                && StringUtils.isNotBlank(worker.getTcmInfo().getAgentName())
                ? worker.getTcmInfo().getAgentName() : processId;
    }

    private String resolveServiceNode(WorkerDto worker, String processId) {
        if (worker == null) {
            return processId;
        }
        if (StringUtils.isNotBlank(worker.getHostname())) {
            return worker.getHostname();
        }
        return StringUtils.isNotBlank(worker.getWorkerIp()) ? worker.getWorkerIp() : processId;
    }


    private Boolean shouldRecord() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();
        String userAgent = request.getHeader("User-Agent");
        if (StringUtils.isNotEmpty(userAgent) && (userAgent.contains("Java") || userAgent.contains("java") || userAgent.contains("nodejs"))) {
            return false;
        }
        return true;
    }
}
