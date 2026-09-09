package com.tapdata.tm.sso.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One-time SSO login codes. TTL 60 seconds, single use, not equivalent to a long-lived access token.
 */
@Service
public class LoginCodeService {

	public static final int TTL_SECONDS = 60;

	private final Map<String, Entry> codes = new ConcurrentHashMap<>();

	public String issue(String tokenId) {
		purgeExpired();
		String code = UUID.randomUUID().toString().replace("-", "");
		codes.put(code, new Entry(tokenId, Instant.now().plusSeconds(TTL_SECONDS)));
		return code;
	}

	public String redeem(String code) {
		if (code == null || code.isBlank()) {
			return null;
		}
		Entry entry = codes.remove(code);
		if (entry == null) {
			return null;
		}
		if (Instant.now().isAfter(entry.expiresAt())) {
			return null;
		}
		return entry.tokenId();
	}

	void putExpiredForTest(String code, String tokenId) {
		codes.put(code, new Entry(tokenId, Instant.now().minusSeconds(1)));
	}

	private void purgeExpired() {
		Instant now = Instant.now();
		codes.entrySet().removeIf(e -> now.isAfter(e.getValue().expiresAt()));
	}

	private record Entry(String tokenId, Instant expiresAt) {
	}
}
