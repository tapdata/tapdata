package com.tapdata.tm.base.security;

/**
 * Inbound compatibility for URL {@code access_token}. Outbound clients must never
 * emit query tokens regardless of this mode.
 */
public enum UrlTokenMode {
	COMPAT,
	WARN,
	REJECT;

	public boolean acceptsUrlToken() {
		return this != REJECT;
	}

	public static UrlTokenMode from(String raw) {
		if (raw == null || raw.isBlank()) {
			return COMPAT;
		}
		try {
			return UrlTokenMode.valueOf(raw.trim().toUpperCase());
		} catch (IllegalArgumentException e) {
			return COMPAT;
		}
	}
}
