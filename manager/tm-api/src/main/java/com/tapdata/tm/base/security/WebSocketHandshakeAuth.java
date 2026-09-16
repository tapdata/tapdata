package com.tapdata.tm.base.security;

import jakarta.servlet.http.HttpServletRequest;

/**
 * WebSocket upgrade-time token policy. Same conflict / URL-token rules as REST,
 * but missing tokens still allow the handshake (legacy anonymous sessions).
 */
public final class WebSocketHandshakeAuth {

	private WebSocketHandshakeAuth() {
	}

	public static Decision evaluate(HttpServletRequest request, UrlTokenMode mode) {
		if (request != null) {
			String userId = request.getHeader("user_id");
			if (userId != null && !userId.isBlank()) {
				return Decision.allow(AccessTokenResolution.missing());
			}
		}
		AccessTokenResolution resolution = AccessTokenResolver.resolve(request, mode, false, true);
		return switch (resolution.getStatus()) {
			case CONFLICT, INVALID_BEARER, URL_TOKEN_REJECTED -> Decision.reject(resolution);
			default -> Decision.allow(resolution);
		};
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
