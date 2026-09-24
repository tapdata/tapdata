package com.tapdata.tm.base.security;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensitiveDataRedactorTest {

	@Test
	void redactsAuthorizationAndCookieHeadersCaseInsensitive() {
		assertEquals("[REDACTED]", SensitiveDataRedactor.redactHeaderValue("Authorization", "Bearer abc.def"));
		assertEquals("[REDACTED]", SensitiveDataRedactor.redactHeaderValue("COOKIE", "access_token=abc"));
		assertEquals("[REDACTED]", SensitiveDataRedactor.redactHeaderValue("access_token", "secret"));
		assertEquals("text/json", SensitiveDataRedactor.redactHeaderValue("Content-Type", "text/json"));
	}

	@Test
	void redactsAccessTokenInsideRefererLocationAndLink() {
		String referer = SensitiveDataRedactor.redactHeaderValue(
				"referer", "http://127.0.0.1:5173/sso/callback?access_token=PLAINTEXT&x=1");
		assertEquals("http://127.0.0.1:5173/sso/callback?access_token=[REDACTED]&x=1", referer);

		String hashReferer = SensitiveDataRedactor.redactHeaderValue(
				"Referer", "http://127.0.0.1:5173/#/sso-callback?access_token=jwt.abc");
		assertEquals("http://127.0.0.1:5173/#/sso-callback?access_token=[REDACTED]", hashReferer);

		assertEquals(
				"https://tapdata/app?access_token=[REDACTED]",
				SensitiveDataRedactor.redactHeaderValue("Location", "https://tapdata/app?access_token=secret"));

		String link = SensitiveDataRedactor.redactHeaderValue(
				"Link", "<http://tm/api?ACCESS_TOKEN=abc>; rel=\"next\"");
		assertEquals("<http://tm/api?ACCESS_TOKEN=[REDACTED]>; rel=\"next\"", link);

		assertEquals(
				"http://127.0.0.1:5173",
				SensitiveDataRedactor.redactHeaderValue("origin", "http://127.0.0.1:5173"));
		assertEquals(
				"http://127.0.0.1:5173/#/sso-callback?login_code=once",
				SensitiveDataRedactor.redactHeaderValue(
						"referer", "http://127.0.0.1:5173/#/sso-callback?login_code=once"));
	}

	@Test
	void redactsAccessTokenQueryIncludingEncodedAndCaseVariants() {
		assertEquals("name=test&access_token=[REDACTED]",
				SensitiveDataRedactor.redactQuery("name=test&access_token=token%2Bvalue"));
		assertEquals("ACCESS_TOKEN=[REDACTED]", SensitiveDataRedactor.redactQuery("ACCESS_TOKEN=secret"));
	}

	@Test
	void redactsUrlQueryAndPreservesPath() {
		String redacted = SensitiveDataRedactor.redactUrl("https://tm/api/users/1?access_token=jwt.abc&x=1");
		assertTrue(redacted.contains("access_token=[REDACTED]"));
		assertTrue(redacted.contains("x=1"));
		assertFalse(redacted.contains("jwt.abc"));
	}

	@Test
	void redactsNestedJsonSecrets() {
		String json = "{\"user\":\"a\",\"nested\":{\"refresh_token\":\"r1\",\"access_token\":\"t1\"},\"authorization\":\"Bearer x\",\"password\":\"Secret123\",\"accesscode\":\"ac-secret\"}";
		String redacted = SensitiveDataRedactor.redactJsonOrNull(json);
		assertTrue(redacted.contains("\"access_token\":\"[REDACTED]\""));
		assertTrue(redacted.contains("\"refresh_token\":\"[REDACTED]\""));
		assertTrue(redacted.contains("\"authorization\":\"[REDACTED]\""));
		assertTrue(redacted.contains("\"password\":\"[REDACTED]\""));
		assertTrue(redacted.contains("\"accesscode\":\"[REDACTED]\""));
		assertFalse(redacted.contains("t1"));
		assertFalse(redacted.contains("Secret123"));
		assertFalse(redacted.contains("ac-secret"));
		assertTrue(redacted.contains("\"user\":\"a\""));
	}

	@Test
	void redactAccessTokenInTextDoesNotUseLowercaseIndexes() {
		String value = "https://tm/app?ACCESS_TOKEN=secret&x=1";
		assertEquals("https://tm/app?ACCESS_TOKEN=[REDACTED]&x=1",
				SensitiveDataRedactor.redactAccessTokenInText(value));
	}

	@Test
	void unparseableBodyReturnsNullSoCallerOmitsRawBody() {
		assertNull(SensitiveDataRedactor.redactJsonOrNull("not-json {"));
	}

	@Test
	void redactHeadersMap() {
		Map<String, String> headers = new LinkedHashMap<>();
		headers.put("Authorization", "Bearer abc");
		headers.put("Host", "tm");
		Map<String, Object> out = SensitiveDataRedactor.redactHeaders(headers);
		assertEquals("[REDACTED]", out.get("Authorization"));
		assertEquals("tm", out.get("Host"));
	}
}
