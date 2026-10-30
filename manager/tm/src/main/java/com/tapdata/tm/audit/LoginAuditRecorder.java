package com.tapdata.tm.audit;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
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
	private final UserService userService;

	/** 生产装配：构造器注入，Spring 只会用这一个构造器创建 Bean。 */
	@Autowired
	public LoginAuditRecorder(AuditEventService auditEventService, UserService userService) {
		this.auditEventService = auditEventService;
		this.userService = userService;
	}

	/** 仅供单元/集成测试在无 UserService 时使用；不参与 Spring 装配。 */
	LoginAuditRecorder(AuditEventService auditEventService) {
		this(auditEventService, null);
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

	/**
	 * 凭据换取 token 的成功事件：调用方只有 userId，按用户回填可读标识，
	 * 否则操作人与操作对象都只剩内部 ID（TAP-13018 F-13）。
	 */
	public void succeeded(String userId, String method, AuditSource source) {
		succeeded(userId, displayName(userId), method, source);
	}

	/** 查不到用户或标识异常时返回 null，由页面显示替代值，不影响登录。 */
	String displayName(String userId) {
		if (userService == null || userId == null || !ObjectId.isValid(userId)) {
			return null;
		}
		try {
			UserDetail user = userService.loadUserById(new ObjectId(userId));
			if (user == null) {
				return null;
			}
			return user.getUsername() != null ? user.getUsername() : user.getEmail();
		} catch (Exception e) {
			log.debug("Resolve login audit user name failed: {}", userId, e);
			return null;
		}
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
