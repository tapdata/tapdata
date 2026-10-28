package com.tapdata.tm.audit;

import com.tapdata.tm.config.security.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** 登录与凭据换取 token 的审计：成功记录真实用户，失败记录「未认证」与脱敏尝试账号（需求 US-02）。 */
@Slf4j
@Component
public class LoginAuditRecorder {
	public static final String METHOD_LOCAL = "local";
	public static final String METHOD_LDAP = "ldap";
	public static final String METHOD_SAML = "saml";
	public static final String METHOD_ACCESS_CODE = "access_code";
	public static final String METHOD_OAUTH2 = "oauth2";

	private final AuditEventService auditEventService;

	public LoginAuditRecorder(AuditEventService auditEventService) {
		this.auditEventService = auditEventService;
	}

	public void succeeded(String userId, String username, String method, AuditSource source) {
		AuditEvent event = AuditEvent.of(action(method), AuditOutcome.SUCCESS);
		event.setUserId(userId);
		event.setUsername(username);
		event.setSource(source);
		event.setAuthType(method);
		event.getContext().put("login_method", method);
		event.object(userId, username);
		auditEventService.record(event, null);
	}

	public void succeeded(UserDetail user, String method, AuditSource source) {
		AuditEvent event = AuditEvent.of(action(method), AuditOutcome.SUCCESS);
		event.setSource(source);
		event.setAuthType(method);
		event.getContext().put("login_method", method);
		event.object(user.getUserId(), user.getUsername() != null ? user.getUsername() : user.getEmail());
		auditEventService.record(event, user);
	}

	public void failed(String attemptedAccount, String method, String reason, AuditSource source) {
		AuditEvent event = AuditEvent.of(action(method), AuditOutcome.FAILED);
		event.setSource(source);
		event.setAuthType(method);
		event.setAttemptedAccount(AuditRedactor.maskAccount(attemptedAccount));
		event.setReason(reason);
		event.getContext().put("login_method", method);
		auditEventService.record(event, null);
	}

	private static AuditAction action(String method) {
		return METHOD_ACCESS_CODE.equals(method) || METHOD_OAUTH2.equals(method) ? AuditAction.API_AUTHENTICATE : AuditAction.LOGIN;
	}
}
