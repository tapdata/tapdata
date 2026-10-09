package com.tapdata.tm.ws.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.socket.WebSocketHandler;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class WebSocketAuthHandshakeInterceptorTest {

	private WebSocketAuthHandshakeInterceptor interceptor;
	private WebSocketHandler handler;

	@BeforeEach
	void setUp() {
		interceptor = new WebSocketAuthHandshakeInterceptor();
		handler = mock(WebSocketHandler.class);
		ReflectionTestUtils.setField(interceptor, "urlTokenModeValue", "COMPAT");
	}

	@Test
	void bearerWinsOverDifferentQueryAndStoresBearer() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("Authorization", "Bearer token-a");
		req.setQueryString("access_token=token-b");
		HandshakeCall call = handshake(req);

		assertTrue(call.ok);
		assertEquals("token-a", call.attributes.get(WebSocketAuthHandshakeInterceptor.ACCESS_TOKEN_ATTRIBUTE));
		assertNull(call.servletResponse.getHeader("Deprecation"));
	}

	@Test
	void rejectModeQueryOnlyAbortsHandshakeWith401() throws Exception {
		ReflectionTestUtils.setField(interceptor, "urlTokenModeValue", "REJECT");
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setQueryString("agentId=h_flow_engine&access_token=valid");
		HandshakeCall call = handshake(req);

		assertFalse(call.ok);
		assertEquals(401, call.servletResponse.getStatus());
	}

	@Test
	void compatQueryStoresTokenAndDeprecationHeaders() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setQueryString("access_token=valid");
		HandshakeCall call = handshake(req);

		assertTrue(call.ok);
		assertEquals("valid", call.attributes.get(WebSocketAuthHandshakeInterceptor.ACCESS_TOKEN_ATTRIBUTE));
		assertEquals("true", call.servletResponse.getHeader("Deprecation"));
		assertEquals("query", call.servletResponse.getHeader("X-Auth-Source"));
	}

	@Test
	void bearerOnlyAllowsHandshake() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("Authorization", "Bearer valid");
		HandshakeCall call = handshake(req);

		assertTrue(call.ok);
		assertEquals("valid", call.attributes.get(WebSocketAuthHandshakeInterceptor.ACCESS_TOKEN_ATTRIBUTE));
		assertNull(call.servletResponse.getHeader("Deprecation"));
	}

	@Test
	void anonymousStillAllowed() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setQueryString("agentId=h_flow_engine");
		HandshakeCall call = handshake(req);

		assertTrue(call.ok);
		assertEquals(200, call.servletResponse.getStatus());
	}

	@Test
	void cookieFromCrossSiteOriginAbortsHandshakeWith401() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setServerName("tm.example.com");
		req.addHeader("Origin", "https://evil.com");
		req.setCookies(new jakarta.servlet.http.Cookie("access_token", "browser-tok"));
		HandshakeCall call = handshake(req);

		assertFalse(call.ok);
		assertEquals(401, call.servletResponse.getStatus());
		assertNull(call.attributes.get(WebSocketAuthHandshakeInterceptor.ACCESS_TOKEN_ATTRIBUTE));
	}

	@Test
	void cookieStoresTokenWithoutDeprecation() throws Exception {
		ReflectionTestUtils.setField(interceptor, "urlTokenModeValue", "REJECT");
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setServerName("localhost");
		req.addHeader("Origin", "http://localhost:5173");
		req.setQueryString("id=client-1");
		req.setCookies(new jakarta.servlet.http.Cookie("access_token", "browser-tok"));
		HandshakeCall call = handshake(req);

		assertTrue(call.ok);
		assertEquals("browser-tok", call.attributes.get(WebSocketAuthHandshakeInterceptor.ACCESS_TOKEN_ATTRIBUTE));
		assertNull(call.servletResponse.getHeader("Deprecation"));
	}

	private HandshakeCall handshake(MockHttpServletRequest req) throws Exception {
		ServletServerHttpRequest serverReq = new ServletServerHttpRequest(req);
		MockHttpServletResponse servletResponse = new MockHttpServletResponse();
		ServletServerHttpResponse serverResp = new ServletServerHttpResponse(servletResponse);
		Map<String, Object> attributes = new HashMap<>();
		boolean ok = interceptor.beforeHandshake(serverReq, serverResp, handler, attributes);
		serverResp.close();
		return new HandshakeCall(ok, servletResponse, attributes);
	}

	private record HandshakeCall(boolean ok, MockHttpServletResponse servletResponse, Map<String, Object> attributes) {
	}
}
