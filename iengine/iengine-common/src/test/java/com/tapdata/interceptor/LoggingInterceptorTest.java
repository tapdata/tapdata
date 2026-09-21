package com.tapdata.interceptor;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggingInterceptorTest {

	@Test
	void credentialIssuingPathsMatchEngineAndTmUrls() {
		assertTrue(LoggingInterceptor.isCredentialIssuingPath(URI.create("http://tm:3030/api/users/generatetoken")));
		assertTrue(LoggingInterceptor.isCredentialIssuingPath(URI.create("http://tm:3030/tm/api/users/login")));
		assertTrue(LoggingInterceptor.isCredentialIssuingPath("/api/users/refreshToken"));
		assertFalse(LoggingInterceptor.isCredentialIssuingPath(URI.create("http://tm:3030/api/Task")));
		assertFalse(LoggingInterceptor.isCredentialIssuingPath((URI) null));
		assertFalse(LoggingInterceptor.isCredentialIssuingPath((String) null));
	}

	@Test
	void generatetokenResponseBodyIsOmittedEvenWhenTokenSitsInId() {
		URI uri = URI.create("http://127.0.0.1:3030/api/users/generatetoken");
		String body = "{\"data\":{\"id\":\"eyJhbGciOiJIUzI1NiJ9.payload.sig\",\"ttl\":16000}}";
		assertEquals("[omitted credential response body]", LoggingInterceptor.formatResponseBody(uri, body));
	}

	@Test
	void otherJsonResponsesStillRedactAccessTokenKeys() {
		URI uri = URI.create("http://127.0.0.1:3030/api/Task");
		String body = "{\"name\":\"t1\",\"access_token\":\"secret\"}";
		String out = LoggingInterceptor.formatResponseBody(uri, body);
		assertTrue(out.contains("[REDACTED]"), out);
		assertFalse(out.contains("secret"), out);
		assertTrue(out.contains("t1"), out);
	}
}
