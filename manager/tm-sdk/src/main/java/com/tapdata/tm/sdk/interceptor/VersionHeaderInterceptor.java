package com.tapdata.tm.sdk.interceptor;

import com.tapdata.tm.sdk.util.Version;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.StringUtils;

import java.io.IOException;

/**
 * 请求头增加版本信息
 */
public class VersionHeaderInterceptor implements ClientHttpRequestInterceptor {

	private static final String HTTP_HEADER_USER_AGENT = "User-Agent";


	@Override
	public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
		String version = Version.get();
		if (!StringUtils.isEmpty(version)) {
			request.getHeaders().add(HTTP_HEADER_USER_AGENT, version);
		}
		return execution.execute(request, body);
	}
}
