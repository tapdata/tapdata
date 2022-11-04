package com.tapdata.tm.base.filter;

import cn.hutool.extra.servlet.ServletUtil;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.ThreadLocalUtils;
import com.tapdata.tm.utils.ThrowableUtils;
import com.tapdata.tm.utils.WebUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Enumeration;

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

		HttpServletRequest httpServletRequest = new HttpServletRequestWrapper((HttpServletRequest) servletRequest);
		HttpServletResponse httpServletResponse = new HttpServletResponseWrapper((HttpServletResponse) servletResponse);

		String requestURI = httpServletRequest.getRequestURI();
		if (Lists.of("/api/pdk/jar", "/api/pdk/icon", "/api/pdk/doc").contains(requestURI)) {
			filterChain.doFilter(servletRequest, servletResponse);
			return;
		}

		ThreadLocalUtils.set(ThreadLocalUtils.USER_LOCALE, WebUtils.getLocale((HttpServletRequest) servletRequest));
		String reqId = ResponseMessage.generatorReqId();
		String ip = ServletUtil.getClientIP(httpServletRequest);
		ThreadLocalUtils.set(ThreadLocalUtils.REQUEST_ID, reqId);
		Thread.currentThread().setName(ip + "-" + Thread.currentThread().getId() + "-" + reqId);

		try {
			filterChain.doFilter(httpServletRequest, httpServletResponse);
		} catch (Throwable e){
			log.error("Process request error", ThrowableUtils.getStackTraceByPn(e));
		}
	}

	private void logReq(ServletRequest servletRequest) {
		HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
		log.debug(" > {} {} {}", httpServletRequest.getMethod(), httpServletRequest.getRequestURI(), httpServletRequest.getProtocol());
		Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
		while (headerNames.hasMoreElements()) {
			String headerName = headerNames.nextElement();
			String headerValue = httpServletRequest.getHeader(headerName);
			log.debug(" > {}: {}", headerName, headerValue);
		}
		try {
			if (httpServletRequest.getQueryString() != null)
				log.debug(" > query: {}", URLDecoder.decode(httpServletRequest.getQueryString(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		if (servletRequest instanceof HttpServletRequestWrapper) {
			try {
				String requestBody = ((HttpServletRequestWrapper) servletRequest).getContentAsString();

				log.debug(" > {}", requestBody);
				log.debug(" > ");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}
	private void logRes(ServletResponse servletResponse) {
		HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
		log.debug(" < {}", httpServletResponse.getStatus());
		httpServletResponse.getHeaderNames().forEach(headerName -> {
			log.debug(" < {}: {}", headerName, httpServletResponse.getHeader(headerName));
		});
		if (servletResponse instanceof HttpServletResponseWrapper) {
			try {
				String content = ((HttpServletResponseWrapper) servletResponse).getContentAsString();
				log.debug(" < {}", content);
				log.debug(" <");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void destroy() {

	}
}
