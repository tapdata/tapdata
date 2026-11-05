package com.tapdata.tm.base.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebSocketHandshakeAuthTest {

	@BeforeEach
	void resetMetrics() {
		AuthTokenMetrics.reset();
	}

	@Test
	void rejectQueryOnlyInRejectMode() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setQueryString("agentId=h_flow_engine&access_token=valid");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.REJECT);
		assertTrue(d.rejected());
		assertEquals(AccessTokenResolution.Status.URL_TOKEN_REJECTED, d.resolution().getStatus());
		assertEquals(1, AuthTokenMetrics.rejected());
	}

	@Test
	void acceptQueryOnlyInCompatAndMarkDeprecated() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setQueryString("access_token=valid");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertFalse(d.rejected());
		assertTrue(d.queryDeprecated());
		assertEquals(AccessTokenSource.QUERY, d.resolution().getSource());
		assertEquals(1, AuthTokenMetrics.accepted());
	}

	@Test
	void bearerWinsOverDifferentQueryInCompat() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("Authorization", "Bearer token-a");
		req.setQueryString("access_token=token-b");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertFalse(d.rejected());
		assertEquals(AccessTokenSource.BEARER, d.resolution().getSource());
		assertEquals("token-a", d.resolution().getToken());
		assertEquals(0, AuthTokenMetrics.conflicts());
	}

	@Test
	void bearerWinsOverDifferentQueryInRejectMode() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("Authorization", "Bearer token-a");
		req.setQueryString("access_token=token-b");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.REJECT);
		assertFalse(d.rejected());
		assertEquals(AccessTokenSource.BEARER, d.resolution().getSource());
		assertEquals("token-a", d.resolution().getToken());
	}

	@Test
	void allowMatchingBearerAndQuery() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("Authorization", "Bearer same");
		req.setQueryString("access_token=same");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertFalse(d.rejected());
		assertFalse(d.queryDeprecated());
		assertEquals(AccessTokenSource.BEARER, d.resolution().getSource());
	}

	@Test
	void allowAnonymousWhenNoToken() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setQueryString("agentId=h_flow_engine");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.REJECT);
		assertFalse(d.rejected());
		assertEquals(AccessTokenResolution.Status.MISSING, d.resolution().getStatus());
	}

	@Test
	void userIdHeaderBypassesUrlTokenReject() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("user_id", "62bc5008d4958d013d97c7a6");
		req.setQueryString("access_token=valid");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.REJECT);
		assertFalse(d.rejected());
	}

	@Test
	void rejectInvalidBearer() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.addHeader("Authorization", "Bearer");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertTrue(d.rejected());
		assertEquals(AccessTokenResolution.Status.INVALID_BEARER, d.resolution().getStatus());
	}

	@Test
	void cookieAuthenticatesBrowserWebsocketWithoutQueryToken() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setServerName("localhost");
		req.addHeader("Origin", "http://localhost:5173");
		req.setQueryString("id=0f8b427d-0f3d-4a2d-af84-114dd7e7eeaa");
		req.setCookies(new jakarta.servlet.http.Cookie("access_token", "browser-tok"));

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.REJECT);
		assertFalse(d.rejected());
		assertFalse(d.queryDeprecated());
		assertEquals(AccessTokenSource.COOKIE, d.resolution().getSource());
		assertEquals("browser-tok", d.resolution().getToken());
	}

	@Test
	void cookieFromCrossSiteOriginIsRejected() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setServerName("tm.example.com");
		req.addHeader("Origin", "https://evil.com");
		req.setCookies(new jakarta.servlet.http.Cookie("access_token", "browser-tok"));

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertTrue(d.rejected());
		assertEquals(AccessTokenSource.COOKIE, d.resolution().getSource());
	}

	@Test
	void cookieWithoutOriginIsRejected() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setCookies(new jakarta.servlet.http.Cookie("access_token", "browser-tok"));

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertTrue(d.rejected());
	}

	@Test
	void cookieFromConfiguredExtraOriginIsAllowed() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setServerName("tm.example.com");
		req.addHeader("Origin", "https://console.example.com");
		req.setCookies(new jakarta.servlet.http.Cookie("access_token", "browser-tok"));

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(
				req, UrlTokenMode.COMPAT, java.util.List.of("https://console.example.com"));
		assertFalse(d.rejected());
		assertEquals("browser-tok", d.resolution().getToken());
	}

	@Test
	void bearerIgnoresCrossSiteOrigin() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/ws/agent");
		req.setServerName("tm.example.com");
		req.addHeader("Origin", "https://evil.com");
		req.addHeader("Authorization", "Bearer engine-tok");

		WebSocketHandshakeAuth.Decision d = WebSocketHandshakeAuth.evaluate(req, UrlTokenMode.COMPAT);
		assertFalse(d.rejected());
		assertEquals(AccessTokenSource.BEARER, d.resolution().getSource());
	}
}
