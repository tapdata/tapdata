package com.tapdata.http.server.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.tapdata.http.server.HttpExchangeWrapper;
import com.tapdata.http.server.HttpServerRequest;
import com.tapdata.http.server.HttpServerResponse;
import com.tapdata.http.server.action.Action;

import java.io.IOException;

/**
 * Action处理器，用于将HttpHandler转换为Action形式
 *
 * @author looly
 * @since 5.2.6
 */
public class ActionHandler implements HttpHandler {

	private final Action action;

	/**
	 * 构造
	 *
	 * @param action Action
	 */
	public ActionHandler(Action action) {
		this.action = action;
	}

	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		final HttpServerRequest request;
		final HttpServerResponse response;
		if (httpExchange instanceof HttpExchangeWrapper) {
			// issue#3343 当使用Filter时，可能读取了请求参数，此时使用共享的req和res，可复用缓存
			final HttpExchangeWrapper wrapper = (HttpExchangeWrapper) httpExchange;
			request = wrapper.getRequest();
			response = wrapper.getResponse();
		} else {
			request = new HttpServerRequest(httpExchange);
			response = new HttpServerResponse(httpExchange);
		}
		action.doAction(request, response);
		httpExchange.close();
	}
}
