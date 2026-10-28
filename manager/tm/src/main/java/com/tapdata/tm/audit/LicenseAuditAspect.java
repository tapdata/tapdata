package com.tapdata.tm.audit;

import com.tapdata.tm.base.dto.ResponseMessage;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/** License 导入/替换的审计。License 控制器来自运行时加载的 tapdata-license-client，这里按类名切入，不依赖其源码。 */
@Aspect
@Component
public class LicenseAuditAspect {
	private final AuditEventService auditEventService;

	public LicenseAuditAspect(AuditEventService auditEventService) {
		this.auditEventService = auditEventService;
	}

	@Around("execution(* com.tapdata.tm.license.controller.LicenseController.upload(..))")
	public Object aroundUpload(ProceedingJoinPoint joinPoint) throws Throwable {
		Object result;
		try {
			result = joinPoint.proceed();
		} catch (Throwable e) {
			record(AuditOutcome.FAILED, AuditAspect.reasonOfPublic(e));
			throw e;
		}
		boolean ok = !(result instanceof ResponseMessage<?> rm) || ResponseMessage.OK.equals(rm.getCode());
		record(ok ? AuditOutcome.SUCCESS : AuditOutcome.FAILED, ok ? null : ((ResponseMessage<?>) result).getCode());
		return result;
	}

	private void record(AuditOutcome outcome, String reason) {
		try {
			AuditEvent event = AuditEvent.of(AuditAction.LICENSE_IMPORT, outcome);
			event.object(null, "License");
			event.setReason(reason);
			auditEventService.record(event, AuditAspect.loginUser());
		} catch (Exception ignore) {
			// 审计失败不改变 License 导入结果
		}
	}
}
