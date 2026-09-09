package com.tapdata.tm.base.security;

import com.tapdata.tm.base.filter.HttpServletRequestWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessTokenResolverTest {

	@BeforeEach
	void resetMetrics() {
		AuthTokenMetrics.reset();
	}

	@Test
	void bearerHasHighestPriorityWhenTokensMatch() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.addHeader("Authorization", "Bearer same");
		req.addHeader("access_token", "same");
		req.setQueryString("access_token=same");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals(AccessTokenResolution.Status.FOUND, r.getStatus());
		assertEquals("same", r.getToken());
		assertEquals(AccessTokenSource.BEARER, r.getSource());
	}

	@Test
	void bearerIsCaseInsensitiveAndTrimsWhitespace() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.addHeader("Authorization", "  bearer   tok-1  ");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals("tok-1", r.getToken());
		assertEquals(AccessTokenSource.BEARER, r.getSource());
	}

	@Test
	void multipleBearerValuesAreInvalid() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.addHeader("Authorization", "Bearer a b");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals(AccessTokenResolution.Status.INVALID_BEARER, r.getStatus());
	}

	@Test
	void basicAuthorizationIsIgnoredByBearerParser() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.addHeader("Authorization", "Basic dXNlcjpwYXNz");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals(AccessTokenResolution.Status.MISSING, r.getStatus());
	}

	@Test
	void headerBeatsBodyAndQueryWhenTokensMatch() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/x");
		req.addHeader("access_token", "same");
		req.setContentType(MediaType.APPLICATION_JSON_VALUE);
		req.setContent("{\"access_token\":\"same\"}".getBytes());
		req.setQueryString("access_token=same");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals("same", r.getToken());
		assertEquals(AccessTokenSource.HEADER, r.getSource());
	}

	@Test
	void jsonBodyBeatsQueryWhenTokensMatch() throws Exception {
		MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/x");
		req.setContentType(MediaType.APPLICATION_JSON_VALUE);
		req.setContent("{\"access_token\":\"from-body\"}".getBytes());
		req.setQueryString("access_token=from-body");

		AccessTokenResolution r = AccessTokenResolver.resolve(new HttpServletRequestWrapper(req), UrlTokenMode.COMPAT);
		assertEquals("from-body", r.getToken());
		assertEquals(AccessTokenSource.BODY, r.getSource());
	}

	@Test
	void loginInterceptorMustNotConsumeJsonBodyWithoutAccessToken() throws Exception {
		byte[] json = "{\"singletonLock\":\"abc\"}".getBytes(StandardCharsets.UTF_8);
		MockHttpServletRequest inner = new MockHttpServletRequest("POST", "/api/Workers/singleton-lock/upsertWithWhere");
		inner.setContentType(MediaType.APPLICATION_JSON_VALUE);
		inner.setContent(json);
		HttpServletRequestWrapper cached = new HttpServletRequestWrapper(inner);
		jakarta.servlet.http.HttpServletRequestWrapper outer =
				new jakarta.servlet.http.HttpServletRequestWrapper(cached);

		AccessTokenResolution r = AccessTokenResolver.resolve(outer, UrlTokenMode.COMPAT);
		assertEquals(AccessTokenResolution.Status.MISSING, r.getStatus());

		String remaining = new String(outer.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		assertEquals("{\"singletonLock\":\"abc\"}", remaining);
	}

	@Test
	void headerOnly() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.addHeader("access_token", "from-header");
		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals("from-header", r.getToken());
		assertEquals(AccessTokenSource.HEADER, r.getSource());
	}

	@Test
	void queryAcceptedInCompatAndCounted() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.setQueryString("name=test&access_token=token%2Bvalue");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.COMPAT);
		assertEquals("token+value", r.getToken());
		assertEquals(AccessTokenSource.QUERY, r.getSource());
		assertEquals(1, AuthTokenMetrics.accepted());
	}

	@Test
	void queryRejectedInRejectModeWhenItIsTheOnlySource() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.setQueryString("access_token=valid");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.REJECT);
		assertEquals(AccessTokenResolution.Status.URL_TOKEN_REJECTED, r.getStatus());
		assertEquals(1, AuthTokenMetrics.rejected());
		assertNull(r.getToken());
	}

	@Test
	void differentTokensConflict() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.addHeader("Authorization", "Bearer A");
		req.setQueryString("access_token=B");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.WARN);
		assertEquals(AccessTokenResolution.Status.CONFLICT, r.getStatus());
		assertEquals(1, AuthTokenMetrics.conflicts());
	}

	@Test
	void warnModeStillAcceptsQuery() {
		MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/x");
		req.setQueryString("access_token=q");

		AccessTokenResolution r = AccessTokenResolver.resolve(req, UrlTokenMode.WARN);
		assertTrue(r.isFound());
		assertEquals(AccessTokenSource.QUERY, r.getSource());
	}
}
