package com.tapdata.tm.audit;

import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.config.security.UserDetail;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 记录权限拒绝与认证失败：NotAuthorized 始终记录；NotLogin 仅在请求携带了凭据时记录（匿名请求不记录），
 * 同一来源与凭据在 5 分钟内只记录一次，避免前端会话过期轮询产生噪音。@Audited 接口内的拒绝由切面按业务动作记录。
 */
@Component
public class AccessFailureAuditor {
	static final long AUTH_FAILURE_WINDOW_MS = 5 * 60_000L;
	private static final int MAX_KEYS = 10_000;

	private final ObjectProvider<AuditEventService> auditEventService;
	private final Map<String, Long> recentAuthFailures = new ConcurrentHashMap<>();

	public AccessFailureAuditor(ObjectProvider<AuditEventService> auditEventService) {
		this.auditEventService = auditEventService;
	}

	public void onBizException(String errorCode, HttpServletRequest request) {
		try {
			AuditEventService service = auditEventService.getIfAvailable();
			AuditRequestContext context = AuditRequestContext.current();
			if (service == null || request == null || (context != null && (context.inAuditedScope() || context.isRecorded()))) {
				return;
			}
			if ("NotAuthorized".equals(errorCode)) {
				recordDenied(service, request);
			} else if ("NotLogin".equals(errorCode) && context != null && context.isCredentialPresented()) {
				recordAuthFailure(service, request, context);
			}
		} catch (Exception ignore) {
			// 审计失败不改变原错误响应
		}
	}

	private void recordDenied(AuditEventService service, HttpServletRequest request) {
		String uri = StringUtils.defaultString(request.getRequestURI());
		boolean auditResource = uri.startsWith("/api/audit-events") || uri.startsWith("/api/UserLogs");
		AuditEvent event = AuditEvent.of(auditResource ? AuditAction.AUDIT_ACCESS_DENIED : AuditAction.ACCESS_DENIED, AuditOutcome.FAILED);
		event.setReason("NotAuthorized");
		event.object(null, resourceName(uri));
		Object user = request.getAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE);
		service.record(event, user instanceof UserDetail detail ? detail : null);
	}

	private void recordAuthFailure(AuditEventService service, HttpServletRequest request, AuditRequestContext context) {
		String key = context.getSourceIp() + "|" + StringUtils.defaultString(credentialFingerprint(request));
		long now = System.currentTimeMillis();
		Long last = recentAuthFailures.get(key);
		if (last != null && now - last < AUTH_FAILURE_WINDOW_MS) {
			return;
		}
		if (recentAuthFailures.size() > MAX_KEYS) {
			recentAuthFailures.clear();
		}
		recentAuthFailures.put(key, now);
		AuditEvent event = AuditEvent.of(AuditAction.API_AUTHENTICATE, AuditOutcome.FAILED);
		event.setReason("NotLogin");
		event.setSource(isBrowser(context.getUserAgent()) ? AuditSource.UI : AuditSource.OPENAPI);
		event.setCredentialRef(AuditRedactor.credentialRef(credentialFingerprint(request)));
		event.object(null, resourceName(request.getRequestURI()));
		service.record(event, null);
	}

	static String credentialFingerprint(HttpServletRequest request) {
		String value = request.getHeader("Authorization");
		if (StringUtils.isBlank(value)) {
			value = request.getHeader("access_token");
		}
		if (value != null && value.contains(" ")) {
			value = value.substring(value.indexOf(' ') + 1);
		}
		return value;
	}

	static boolean isBrowser(String userAgent) {
		return userAgent != null && userAgent.contains("Mozilla");
	}

	/** 只保留资源路径的前两级（如 /api/Task），不记录完整 URL、查询参数或 ID。 */
	static String resourceName(String uri) {
		if (uri == null) {
			return null;
		}
		String[] parts = uri.split("/");
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i < parts.length && i <= 2; i++) {
			sb.append('/').append(parts[i]);
		}
		return sb.toString();
	}
}
