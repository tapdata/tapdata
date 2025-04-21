package com.tapdata.http.server.filter;

import com.sun.net.httpserver.Filter;
import com.tapdata.http.server.HttpServerRequest;
import com.tapdata.http.server.HttpServerResponse;

import java.io.IOException;

/**
 * 过滤器接口，用于简化{@link Filter} 使用
 *
 * @author looly
 * @since 5.5.7
 */
@FunctionalInterface
public interface HttpFilter {

	/**
	 * 执行过滤
	 * @param req {@link HttpServerRequest} 请求对象，用于获取请求内容
	 * @param res {@link HttpServerResponse} 响应对象，用于写出内容
	 * @param chain {@link Filter.Chain}
	 * @throws IOException IO异常
	 */
	void doFilter(HttpServerRequest req, HttpServerResponse res, Filter.Chain chain) throws IOException;
}
