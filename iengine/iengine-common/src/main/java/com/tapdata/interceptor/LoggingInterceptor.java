package com.tapdata.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/3/23 下午5:44
 * @description
 */
public class LoggingInterceptor implements ClientHttpRequestInterceptor {

	private Logger log = LogManager.getLogger(LoggingInterceptor.class);

	@Override
	public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
		boolean debugger = log.isDebugEnabled();
		long reqTime = System.currentTimeMillis();
		String reqId = UUID.randomUUID().toString();
		request.getHeaders().add("requestId", reqId);
		if (debugger) {
			logRequest(reqId, request, body);
		}
		ClientHttpResponse response = execution.execute(request, body);
		long totalTime = System.currentTimeMillis() - reqTime;

		if (debugger) {
			response = new BufferingClientHttpResponseWrapper(response);
			logResponse(reqId, request, response, totalTime);
		}

		return response;
	}

	private void logRequest(String requestId, HttpRequest request, byte[] body) throws IOException {
		log.debug(requestId + " > URI         : " + request.getURI());
		log.debug(requestId + " > Method      : " + request.getMethod());
		log.debug(requestId + " > Headers     : " + request.getHeaders());
		log.debug(requestId + " > Request body: " + new String(body, "UTF-8"));
	}

	private void logResponse(String requestId, HttpRequest request, ClientHttpResponse response, long ttl) throws IOException {
		log.debug(requestId + " < Status code  : " + response.getStatusCode());
		log.debug(requestId + " < Status text  : " + response.getStatusText());
		log.debug(requestId + " < TTL          : " + ttl + "ms");
		log.debug(requestId + " < Headers      : " + response.getHeaders());
		log.debug(requestId + " < Response body: " + StreamUtils.copyToString(response.getBody(), Charset.defaultCharset()));
	}

}
