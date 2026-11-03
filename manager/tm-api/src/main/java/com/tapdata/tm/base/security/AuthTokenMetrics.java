package com.tapdata.tm.base.security;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-local counters for URL-token migration. Names match
 * {@code auth.url_token.accepted|rejected|conflict}.
 */
public final class AuthTokenMetrics {

	private static final AtomicLong ACCEPTED = new AtomicLong();
	private static final AtomicLong REJECTED = new AtomicLong();
	private static final AtomicLong CONFLICT = new AtomicLong();

	private AuthTokenMetrics() {
	}

	public static void urlTokenAccepted() {
		ACCEPTED.incrementAndGet();
	}

	public static void urlTokenRejected() {
		REJECTED.incrementAndGet();
	}

	public static void conflict() {
		CONFLICT.incrementAndGet();
	}

	public static long accepted() {
		return ACCEPTED.get();
	}

	public static long rejected() {
		return REJECTED.get();
	}

	public static long conflicts() {
		return CONFLICT.get();
	}

	public static void reset() {
		ACCEPTED.set(0);
		REJECTED.set(0);
		CONFLICT.set(0);
	}
}
