package com.tapdata.tm.sso.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LoginCodeServiceTest {

	@Test
	void redeemOnceThenRejectReplay() {
		LoginCodeService service = new LoginCodeService();
		String code = service.issue("tok-1");
		assertEquals("tok-1", service.redeem(code));
		assertNull(service.redeem(code));
	}

	@Test
	void expiredCodeCannotBeRedeemed() {
		LoginCodeService service = new LoginCodeService();
		service.putExpiredForTest("old", "tok-1");
		assertNull(service.redeem("old"));
	}

	@Test
	void unknownCodeReturnsNull() {
		assertNull(new LoginCodeService().redeem("missing"));
	}

	@Test
	void eachIssueIsUnique() {
		LoginCodeService service = new LoginCodeService();
		assertNotEquals(service.issue("t"), service.issue("t"));
	}
}
