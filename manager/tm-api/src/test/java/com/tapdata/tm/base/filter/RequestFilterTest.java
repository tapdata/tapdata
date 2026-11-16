package com.tapdata.tm.base.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestFilterTest {

	@Test
	void credentialIssuingPathsOmitResponseBody() {
		assertTrue(RequestFilter.isCredentialIssuingPath("/api/users/login"));
		assertTrue(RequestFilter.isCredentialIssuingPath("/api/users/generatetoken"));
		assertTrue(RequestFilter.isCredentialIssuingPath("/tm/api/users/refreshAccessCode"));
		assertTrue(RequestFilter.isCredentialIssuingPath("/api/users/access-code/reveal"));
		assertTrue(RequestFilter.isCredentialIssuingPath("/api/users/123/access-code/reveal"));
		assertTrue(RequestFilter.isCredentialIssuingPath("/api/Applications/123/secret/reveal"));
		assertFalse(RequestFilter.isCredentialIssuingPath("/api/Task"));
		assertFalse(RequestFilter.isCredentialIssuingPath(null));
	}
}
