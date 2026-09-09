package com.tapdata.tm.base.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.filter.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

/**
 * Shared inbound token extraction. Priority: Bearer, {@code access_token} header,
 * JSON body {@code access_token}, URL {@code access_token} (COMPAT/WARN only).
 */
public final class AccessTokenResolver {

	public static final String AUTH_SOURCE_ATTRIBUTE = "tapdata.auth_source";
	public static final String ACCESS_TOKEN_HEADER = "access_token";
	private static final String AUTHORIZATION = "Authorization";
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private AccessTokenResolver() {
	}

	public static AccessTokenResolution resolve(HttpServletRequest request, UrlTokenMode mode) {
		return resolve(request, mode, true);
	}

	public static AccessTokenResolution resolve(HttpServletRequest request, UrlTokenMode mode, boolean allowBody) {
		if (request == null) {
			return AccessTokenResolution.missing();
		}
		UrlTokenMode effective = mode == null ? UrlTokenMode.COMPAT : mode;

		BearerParse bearer = parseBearer(request);
		if (bearer.invalid) {
			return AccessTokenResolution.invalidBearer();
		}

		List<Candidate> candidates = new ArrayList<>();
		if (bearer.token != null) {
			candidates.add(new Candidate(bearer.token, AccessTokenSource.BEARER));
		}

		String headerToken = nullIfBlank(request.getHeader(ACCESS_TOKEN_HEADER));
		if (headerToken != null) {
			candidates.add(new Candidate(headerToken, AccessTokenSource.HEADER));
		}

		if (allowBody) {
			String bodyToken = fromJsonBody(request);
			if (bodyToken != null) {
				candidates.add(new Candidate(bodyToken, AccessTokenSource.BODY));
			}
		}

		String queryToken = fromQuery(request);
		if (queryToken != null) {
			if (!effective.acceptsUrlToken()) {
				if (candidates.isEmpty()) {
					AuthTokenMetrics.urlTokenRejected();
					return AccessTokenResolution.urlRejected();
				}
				// URL token present alongside a higher-priority source: conflict if different.
				candidates.add(new Candidate(queryToken, AccessTokenSource.QUERY));
			} else {
				candidates.add(new Candidate(queryToken, AccessTokenSource.QUERY));
			}
		}

		if (candidates.isEmpty()) {
			return AccessTokenResolution.missing();
		}

		String chosenToken = candidates.get(0).token;
		for (Candidate c : candidates) {
			if (!chosenToken.equals(c.token)) {
				AuthTokenMetrics.conflict();
				return AccessTokenResolution.conflict();
			}
		}

		Candidate chosen = candidates.get(0);
		if (chosen.source == AccessTokenSource.QUERY) {
			AuthTokenMetrics.urlTokenAccepted();
		}
		request.setAttribute(AUTH_SOURCE_ATTRIBUTE, chosen.source.name());
		return AccessTokenResolution.found(chosen.token, chosen.source);
	}

	static String fromQuery(HttpServletRequest request) {
		String queryString = request.getQueryString();
		if (queryString == null || queryString.isBlank()) {
			return null;
		}
		for (String parameter : queryString.split("&")) {
			String[] pair = parameter.split("=", 2);
			if (pair.length == 0) {
				continue;
			}
			String key = urlDecode(pair[0]);
			if (!"access_token".equals(key)) {
				continue;
			}
			if (pair.length < 2) {
				return null;
			}
			try {
				return nullIfBlank(URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
			} catch (IllegalArgumentException e) {
				return null;
			}
		}
		return null;
	}

	private static String fromJsonBody(HttpServletRequest request) {
		String contentType = request.getContentType();
		if (contentType == null || !contentType.toLowerCase(Locale.ROOT).contains(MediaType.APPLICATION_JSON_VALUE)) {
			return null;
		}
		String body = readBody(request);
		if (body == null || body.isBlank()) {
			return null;
		}
		try {
			JsonNode root = MAPPER.readTree(body);
			if (root == null || !root.isObject()) {
				return null;
			}
			JsonNode field = root.get("access_token");
			return field == null || field.isNull() ? null : nullIfBlank(field.asText());
		} catch (Exception e) {
			return null;
		}
	}

	private static String readBody(HttpServletRequest request) {
		if (request instanceof HttpServletRequestWrapper wrapper) {
			try {
				return wrapper.getContentAsString();
			} catch (Exception e) {
				return null;
			}
		}
		try {
			byte[] bytes = request.getInputStream().readAllBytes();
			if (bytes.length == 0) {
				return null;
			}
			String encoding = request.getCharacterEncoding();
			return new String(bytes, encoding == null ? StandardCharsets.UTF_8 : java.nio.charset.Charset.forName(encoding));
		} catch (Exception e) {
			return null;
		}
	}

	private static BearerParse parseBearer(HttpServletRequest request) {
		List<String> tokens = new ArrayList<>();
		boolean invalid = false;
		Enumeration<String> values = request.getHeaders(AUTHORIZATION);
		if (values == null || !values.hasMoreElements()) {
			String single = request.getHeader(AUTHORIZATION);
			if (single == null) {
				single = request.getHeader("authorization");
			}
			if (single == null) {
				return BearerParse.none();
			}
			return parseOneAuthorization(single);
		}
		while (values.hasMoreElements()) {
			BearerParse one = parseOneAuthorization(values.nextElement());
			if (one.invalid) {
				invalid = true;
				break;
			}
			if (one.token != null) {
				tokens.add(one.token);
			}
		}
		if (invalid) {
			return BearerParse.invalid();
		}
		if (tokens.isEmpty()) {
			return BearerParse.none();
		}
		String first = tokens.get(0);
		for (String t : tokens) {
			if (!first.equals(t)) {
				return BearerParse.invalid();
			}
		}
		return BearerParse.token(first);
	}

	private static BearerParse parseOneAuthorization(String raw) {
		if (raw == null) {
			return BearerParse.none();
		}
		String trimmed = raw.trim();
		if (trimmed.isEmpty()) {
			return BearerParse.none();
		}
		int space = indexOfWhitespace(trimmed);
		if (space < 0) {
			return BearerParse.invalid();
		}
		String scheme = trimmed.substring(0, space);
		String rest = trimmed.substring(space).trim();
		if (!"Bearer".equalsIgnoreCase(scheme)) {
			return BearerParse.none();
		}
		if (rest.isEmpty() || indexOfWhitespace(rest) >= 0) {
			return BearerParse.invalid();
		}
		return BearerParse.token(rest);
	}

	private static int indexOfWhitespace(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (Character.isWhitespace(s.charAt(i))) {
				return i;
			}
		}
		return -1;
	}

	private static String urlDecode(String raw) {
		try {
			return URLDecoder.decode(raw, StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			return raw;
		}
	}

	private static String nullIfBlank(String s) {
		return s == null || s.isBlank() ? null : s;
	}

	private record Candidate(String token, AccessTokenSource source) {
	}

	private static final class BearerParse {
		final boolean invalid;
		final String token;

		private BearerParse(boolean invalid, String token) {
			this.invalid = invalid;
			this.token = token;
		}

		static BearerParse none() {
			return new BearerParse(false, null);
		}

		static BearerParse invalid() {
			return new BearerParse(true, null);
		}

		static BearerParse token(String token) {
			return new BearerParse(false, token);
		}
	}
}
