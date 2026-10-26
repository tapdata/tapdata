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
		assertFalse(RequestFilter.isCredentialIssuingPath("/api/Task"));
		assertFalse(RequestFilter.isCredentialIssuingPath(null));
	}
}
