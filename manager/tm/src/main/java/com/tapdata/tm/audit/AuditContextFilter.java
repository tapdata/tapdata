package com.tapdata.tm.audit;

import com.tapdata.tm.utils.ThreadLocalUtils;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 为每个请求建立审计上下文：来源 IP（按可信代理规则）、Operation ID、User-Agent 与是否携带凭据。
 * 可信代理通过 tapdata.audit.trusted-proxies 配置 IP 或 CIDR，逗号分隔，默认空。
 */
@Component
@Order(-1)
public class AuditContextFilter implements Filter {
	private final SourceIpResolver resolver;

	public AuditContextFilter(@Value("${tapdata.audit.trusted-proxies:}") String trustedProxies) {
		List<String> entries = Arrays.stream(StringUtils.defaultString(trustedProxies).split(","))
				.map(String::trim).filter(StringUtils::isNotBlank).toList();
		this.resolver = new SourceIpResolver(entries);
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		if (!(request instanceof HttpServletRequest http)) {
			chain.doFilter(request, response);
			return;
		}
		AuditRequestContext.set(build(http));
		try {
			chain.doFilter(request, response);
		} finally {
			AuditRequestContext.clear();
		}
	}

	AuditRequestContext build(HttpServletRequest request) {
		AuditRequestContext context = new AuditRequestContext();
		String forwarded = request.getHeader("X-Forwarded-For");
		SourceIpResolver.Result ip = resolver.resolve(request.getRemoteAddr(), forwarded);
		context.setSourceIp(ip.ip());
		context.setSourceIpNote(ip.note());
		context.setForwardedFor(forwarded);
		context.setUserAgent(request.getHeader("User-Agent"));
		String reqId = ThreadLocalUtils.get(ThreadLocalUtils.REQUEST_ID);
		context.setOperationId(StringUtils.isNotBlank(reqId) ? reqId : UUID.randomUUID().toString());
		context.setCredentialPresented(hasCredential(request));
		return context;
	}

	static boolean hasCredential(HttpServletRequest request) {
		return StringUtils.isNotBlank(request.getHeader("Authorization"))
				|| StringUtils.isNotBlank(request.getHeader("access_token"))
				|| StringUtils.containsIgnoreCase(request.getQueryString(), "access_token=")
				|| StringUtils.isNotBlank(request.getHeader("user_id"));
	}

	SourceIpResolver getResolver() {
		return resolver;
	}
}
