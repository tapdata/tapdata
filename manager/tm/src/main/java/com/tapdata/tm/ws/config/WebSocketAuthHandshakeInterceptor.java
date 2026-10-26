package com.tapdata.tm.ws.config;

import com.tapdata.tm.base.security.AccessTokenResolution;
import com.tapdata.tm.base.security.UrlTokenMode;
import com.tapdata.tm.base.security.WebSocketHandshakeAuth;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Rejects the HTTP upgrade when token sources conflict or URL tokens are forbidden.
 */
@Component
public class WebSocketAuthHandshakeInterceptor implements HandshakeInterceptor {

	public static final String ACCESS_TOKEN_ATTRIBUTE = "tapdata.ws.access_token";

	@Value("${security.auth.url-token-mode:COMPAT}")
	private String urlTokenModeValue = "COMPAT";

	@Value("${security.auth.websocket-allowed-origins:}")
	private String websocketAllowedOrigins = "";

	@Override
	public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
			WebSocketHandler wsHandler, Map<String, Object> attributes) {
		if (!(request instanceof ServletServerHttpRequest servletRequest)) {
			response.setStatusCode(HttpStatus.UNAUTHORIZED);
			return false;
		}
		HttpServletRequest httpRequest = servletRequest.getServletRequest();
		WebSocketHandshakeAuth.Decision decision = WebSocketHandshakeAuth.evaluate(
				httpRequest, UrlTokenMode.from(urlTokenModeValue), extraAllowedOrigins());
		if (decision.rejected()) {
			response.setStatusCode(HttpStatus.UNAUTHORIZED);
			return false;
		}
		AccessTokenResolution resolution = decision.resolution();
		if (resolution != null && resolution.isFound()) {
			attributes.put(ACCESS_TOKEN_ATTRIBUTE, resolution.getToken());
		}
		if (decision.queryDeprecated()) {
			response.getHeaders().set("Deprecation", "true");
			response.getHeaders().set("X-Auth-Source", "query");
		}
		return true;
	}

	@Override
	public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
			WebSocketHandler wsHandler, Exception exception) {
		// no-op
	}

	private List<String> extraAllowedOrigins() {
		if (websocketAllowedOrigins == null || websocketAllowedOrigins.isBlank()) {
			return List.of();
		}
		return Arrays.stream(websocketAllowedOrigins.split(","))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.toList();
	}
}
