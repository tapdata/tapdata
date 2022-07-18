package com.tapdata.interceptor;

import com.tapdata.entity.Version;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * 请求头增加版本信息
 */
public class VersionHeaderInterceptor implements ClientHttpRequestInterceptor {

	private static final String HTTP_HEADER_USER_AGENT = "User-Agent";


	@Override
	public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
		String version = Version.get();
		if (StringUtils.isNotEmpty(version)) {
			request.getHeaders().add(HTTP_HEADER_USER_AGENT, version);
		}
		return execution.execute(request, body);
	}
}
