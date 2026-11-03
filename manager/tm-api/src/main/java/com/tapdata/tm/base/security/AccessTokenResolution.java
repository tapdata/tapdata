package com.tapdata.tm.base.security;

public final class AccessTokenResolution {

	public enum Status {
		FOUND,
		MISSING,
		CONFLICT,
		INVALID_BEARER,
		URL_TOKEN_REJECTED
	}

	private final Status status;
	private final String token;
	private final AccessTokenSource source;

	private AccessTokenResolution(Status status, String token, AccessTokenSource source) {
		this.status = status;
		this.token = token;
		this.source = source;
	}

	public static AccessTokenResolution found(String token, AccessTokenSource source) {
		return new AccessTokenResolution(Status.FOUND, token, source);
	}

	public static AccessTokenResolution missing() {
		return new AccessTokenResolution(Status.MISSING, null, null);
	}

	public static AccessTokenResolution conflict() {
		return new AccessTokenResolution(Status.CONFLICT, null, null);
	}

	public static AccessTokenResolution invalidBearer() {
		return new AccessTokenResolution(Status.INVALID_BEARER, null, null);
	}

	public static AccessTokenResolution urlRejected() {
		return new AccessTokenResolution(Status.URL_TOKEN_REJECTED, null, AccessTokenSource.QUERY);
	}

	public Status getStatus() {
		return status;
	}

	public String getToken() {
		return token;
	}

	public AccessTokenSource getSource() {
		return source;
	}

	public boolean isFound() {
		return status == Status.FOUND;
	}
}
