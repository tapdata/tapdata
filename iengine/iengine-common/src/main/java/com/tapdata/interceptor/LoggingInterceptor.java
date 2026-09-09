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
import java.util.List;
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
		log.debug(requestId + " > URI         : {}", redactUri(request.getURI()));
		log.debug(requestId + " > Method      : {}", request.getMethod());
		log.debug(requestId + " > Headers     : {}", redactHeaders(request.getHeaders()));
		log.debug(requestId + " > Request body: {}", redactBody(body));
	}

	private void logResponse(String requestId, HttpRequest request, ClientHttpResponse response, long ttl) throws IOException {
		log.debug(requestId + " < Status code  : {}", response.getStatusCode());
		log.debug(requestId + " < Status text  : {}", response.getStatusText());
		log.debug(requestId + " < TTL          : {}ms", ttl);
		log.debug(requestId + " < Headers      : {}", redactHeaders(response.getHeaders()));
		log.debug(requestId + " < Response body: {}", StreamUtils.copyToString(response.getBody(), Charset.defaultCharset()));
	}

	private static String redactUri(java.net.URI uri) {
		if (uri == null) {
			return null;
		}
		return String.valueOf(uri).replaceAll("(?i)([?&]access_token=)[^&]*", "$1[REDACTED]");
	}

	private static String redactHeaders(org.springframework.http.HttpHeaders headers) {
		if (headers == null) {
			return null;
		}
		org.springframework.http.HttpHeaders copy = new org.springframework.http.HttpHeaders();
		copy.putAll(headers);
		for (String name : List.of("Authorization", "access_token", "Cookie", "Set-Cookie", "Proxy-Authorization")) {
			if (copy.getFirst(name) != null) {
				copy.set(name, "[REDACTED]");
			}
		}
		return copy.toString();
	}

	private static String redactBody(byte[] body) {
		if (body == null) {
			return "";
		}
		String raw = new String(body, Charset.defaultCharset());
		return raw.replaceAll("(?i)(\"(access_token|authorization|refresh_token|client_secret)\"\\s*:\\s*\")[^\"]*", "$1[REDACTED]");
	}

}
