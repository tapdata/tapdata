package com.tapdata.tm.base.security;

import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;
import java.util.Collection;
import java.util.List;

/**
 * WebSocket upgrade-time token policy. Same Header-wins / URL-token rules as REST,
 * but missing tokens still allow the handshake (legacy anonymous sessions).
 *
 * Cookie auth is same-host only (plus optional extra Origins). Bearer / query / header
 * are unchanged so the engine can still connect with {@code setAllowedOrigins("*")}.
 */
public final class WebSocketHandshakeAuth {

	private WebSocketHandshakeAuth() {
	}

	public static Decision evaluate(HttpServletRequest request, UrlTokenMode mode) {
		return evaluate(request, mode, List.of());
	}

	public static Decision evaluate(HttpServletRequest request, UrlTokenMode mode,
			Collection<String> extraAllowedOrigins) {
		if (request != null) {
			String userId = request.getHeader("user_id");
			if (userId != null && !userId.isBlank()) {
				return Decision.allow(AccessTokenResolution.missing());
			}
		}
		AccessTokenResolution resolution = AccessTokenResolver.resolve(request, mode, false, true);
		return switch (resolution.getStatus()) {
			case CONFLICT, INVALID_BEARER, URL_TOKEN_REJECTED -> Decision.reject(resolution);
			case FOUND -> {
				if (resolution.getSource() == AccessTokenSource.COOKIE
						&& !cookieOriginAllowed(request, extraAllowedOrigins)) {
					yield Decision.reject(resolution);
				}
				yield Decision.allow(resolution);
			}
			default -> Decision.allow(resolution);
		};
	}

	static boolean cookieOriginAllowed(HttpServletRequest request, Collection<String> extraAllowedOrigins) {
		if (request == null) {
			return false;
		}
		String originHeader = request.getHeader("Origin");
		if (originHeader == null || originHeader.isBlank() || "null".equalsIgnoreCase(originHeader)) {
			return false;
		}
		URI origin;
		try {
			origin = URI.create(originHeader);
		} catch (IllegalArgumentException e) {
			return false;
		}
		String originHost = origin.getHost();
		if (originHost == null || originHost.isBlank()) {
			return false;
		}
		String serverName = request.getServerName();
		if (serverName != null && originHost.equalsIgnoreCase(serverName)) {
			return true;
		}
		if (extraAllowedOrigins == null) {
			return false;
		}
		for (String extra : extraAllowedOrigins) {
			if (extra != null && originHeader.equalsIgnoreCase(extra.trim())) {
				return true;
			}
		}
		return false;
	}

	public static final class Decision {
		private final boolean rejected;
		private final AccessTokenResolution resolution;

		private Decision(boolean rejected, AccessTokenResolution resolution) {
			this.rejected = rejected;
			this.resolution = resolution;
		}

		static Decision allow(AccessTokenResolution resolution) {
			return new Decision(false, resolution);
		}

		static Decision reject(AccessTokenResolution resolution) {
			return new Decision(true, resolution);
		}

		public boolean rejected() {
			return rejected;
		}

		public AccessTokenResolution resolution() {
			return resolution;
		}

		public boolean queryDeprecated() {
			return !rejected && resolution != null && resolution.getSource() == AccessTokenSource.QUERY;
		}
	}
}
