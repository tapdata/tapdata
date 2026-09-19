package com.tapdata.tm.base.filter;

import cn.hutool.extra.servlet.JakartaServletUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.security.SensitiveDataRedactor;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.ThreadLocalUtils;
import com.tapdata.tm.utils.WebUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/14 9:13 上午
 * @description
 */
@Component
@Slf4j
@Order(-2)
public class RequestFilter implements Filter {
	@Override
	public void init(FilterConfig filterConfig) throws ServletException {

	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws ServletException, IOException {
		long startTime = System.currentTimeMillis();
		if(org.apache.commons.lang3.StringUtils.isNotBlank(servletRequest.getContentType()) && servletRequest.getContentType().contains("multipart/form-data")){
			filterChain.doFilter(servletRequest, servletResponse);
			return;
		}
		HttpServletRequest httpServletRequest = new HttpServletRequestWrapper((HttpServletRequest) servletRequest);
		HttpServletResponse httpServletResponse = new HttpServletResponseWrapper((HttpServletResponse) servletResponse);

		String requestURI = httpServletRequest.getRequestURI();
		if (Lists.of("/api/pdk/jar", "/api/pdk/icon", "/api/pdk/doc", "/api/pdk/upload/source").contains(requestURI)) {
			filterChain.doFilter(servletRequest, servletResponse);
			return;
		}

		ThreadLocalUtils.set(ThreadLocalUtils.USER_LOCALE, WebUtils.getLocale((HttpServletRequest) servletRequest));
		List<String> values = WebUtils.parseQueryString(httpServletRequest.getQueryString()).get("reqId");
		String reqIdFromHeader = httpServletRequest.getHeader("requestId");
		String reqId;
		if (values != null && values.size() > 0) {
			reqId = values.get(0);
		} else if (StringUtils.isNotBlank(reqIdFromHeader)){
			reqId = reqIdFromHeader;
		} else {
			reqId = ResponseMessage.generatorReqId();
		}
		String ip = JakartaServletUtil.getClientIP(httpServletRequest);
		ThreadLocalUtils.set(ThreadLocalUtils.REQUEST_ID, reqId);
		Thread.currentThread().setName(ip + "-" + Thread.currentThread().getId() + "-" + reqId);

		if (log.isTraceEnabled()) {
			try {
				logReq(httpServletRequest);
			} catch (Exception e) {
				log.debug("Unable to trace request", e);
			}
		}

		try {
			filterChain.doFilter(httpServletRequest, httpServletResponse);
		} catch (Throwable e){
			log.error("Process request error {}", ThrowableUtils.getStackTraceByPn(e));
		} finally {
			log.debug("{} {} {} {}ms ", Thread.currentThread().getName(), httpServletRequest.getMethod(), requestURI, System.currentTimeMillis() - startTime);
		}

		if (log.isTraceEnabled()) {
			try {
				logRes(httpServletResponse, requestURI);
			} catch (Exception e) {
				log.debug("Unable to trace response", e);
			}
		}
	}

	private void logReq(ServletRequest servletRequest) {
		HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
		log.trace(" > {} {} {}", httpServletRequest.getMethod(), httpServletRequest.getRequestURI(), httpServletRequest.getProtocol());
		Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
		while (headerNames.hasMoreElements()) {
			String headerName = headerNames.nextElement();
			String headerValue = httpServletRequest.getHeader(headerName);
			log.trace(" > {}: {}", headerName, SensitiveDataRedactor.redactHeaderValue(headerName, headerValue));
		}
		try {
			if (httpServletRequest.getQueryString() != null)
				log.trace(" > query: {}", SensitiveDataRedactor.redactQuery(
						URLDecoder.decode(httpServletRequest.getQueryString(), "UTF-8")));
		} catch (UnsupportedEncodingException e) {
			log.debug("Unable to decode query string for trace log");
		}
		if (servletRequest instanceof HttpServletRequestWrapper) {
			try {
				String requestBody;
				String contentType = httpServletRequest.getContentType();
				if ( contentType != null && contentType.contains("multipart/form-data")) {
					requestBody = "Ignore log for binary upload!!!!!!";
				} else {
					String raw = ((HttpServletRequestWrapper) servletRequest).getContentAsString();
					String redacted = SensitiveDataRedactor.redactJsonOrNull(raw);
					if (redacted != null) {
						requestBody = redacted;
					} else if (raw == null) {
						requestBody = "";
					} else {
						requestBody = "omitted non-json body length=" + raw.length()
								+ " contentType=" + contentType;
					}
				}

				log.trace(" > {}", requestBody);
				log.trace(" > ");
			} catch (UnsupportedEncodingException e) {
				log.debug("Unable to decode request body for trace log");
			}
		}
	}
	private void logRes(ServletResponse servletResponse, String requestURI) {
		HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
		log.trace(" < {}", httpServletResponse.getStatus());
		httpServletResponse.getHeaderNames().forEach(headerName -> {
			log.trace(" < {}: {}", headerName, SensitiveDataRedactor.redactHeaderValue(headerName, httpServletResponse.getHeader(headerName)));
		});
		String contentType = httpServletResponse.getHeader("Content-Type");
		if (!"application/zip".equals(contentType) && servletResponse instanceof HttpServletResponseWrapper) {
			try {
				if (isCredentialIssuingPath(requestURI)) {
					log.trace(" < [omitted credential response body]");
					log.trace(" <");
					return;
				}
				String content = ((HttpServletResponseWrapper) servletResponse).getContentAsString();
				String redacted = SensitiveDataRedactor.redactJsonOrNull(content);
				log.trace(" < {}", redacted != null ? redacted : content);
				log.trace(" <");
			} catch (UnsupportedEncodingException e) {
				log.debug("Unable to decode response body for trace log");
			}
		}
	}

	static boolean isCredentialIssuingPath(String requestURI) {
		if (requestURI == null || requestURI.isEmpty()) {
			return false;
		}
		return requestURI.contains("/users/login")
				|| requestURI.contains("/users/generatetoken")
				|| requestURI.contains("/users/refreshToken");
	}

	@Override
	public void destroy() {

	}
}
