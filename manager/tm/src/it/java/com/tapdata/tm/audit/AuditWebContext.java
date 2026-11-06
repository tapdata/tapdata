package com.tapdata.tm.audit;

import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.config.security.UserDetail;
import org.springframework.aop.aspectj.annotation.AnnotationAwareAspectJAutoProxyCreator;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/** 模拟一次 HTTP 请求：经真实 AuditContextFilter 建立审计上下文，并按 LoginUserResolver 的缓存约定放入登录用户。 */
final class AuditWebContext {
	private AuditWebContext() {
	}

	static MockHttpServletRequest begin(String trustedProxies, String remoteAddr, String forwardedFor, UserDetail user) {
		MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/Task");
		request.setRemoteAddr(remoteAddr);
		if (forwardedFor != null) {
			request.addHeader("X-Forwarded-For", forwardedFor);
		}
		request.addHeader("User-Agent", "Mozilla/5.0 (Macintosh)");
		if (user != null) {
			request.setAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE, user);
			request.addHeader("Authorization", "Bearer it-token-0000000000000000");
		}
		RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
		AuditRequestContext.set(new AuditContextFilter(trustedProxies).build(request));
		return request;
	}

	static void end() {
		AuditRequestContext.clear();
		RequestContextHolder.resetRequestAttributes();
	}

	/** 以真实 AuditAspect 代理控制器替身。 */
	static GenericApplicationContext aspectContext(AuditItFixture fixture) {
		GenericApplicationContext context = new GenericApplicationContext();
		context.registerBean(AnnotationAwareAspectJAutoProxyCreator.class);
		context.registerBean(AuditAspect.class, () -> new AuditAspect(fixture.auditEventService, fixture.objectResolver, fixture.registry));
		context.registerBean(AuditDemoController.class);
		context.refresh();
		return context;
	}
}
