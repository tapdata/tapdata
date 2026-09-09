package com.tapdata.tm.base.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Pure redaction helpers for logs and audit. Never logs or returns the original secret.
 */
public final class SensitiveDataRedactor {

	public static final String REDACTED = "[REDACTED]";
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final Set<String> SENSITIVE_HEADERS = Set.of(
			"authorization",
			"access_token",
			"cookie",
			"set-cookie",
			"proxy-authorization"
	);
	private static final Set<String> SENSITIVE_JSON_KEYS = Set.of(
			"access_token",
			"authorization",
			"refresh_token",
			"client_secret"
	);

	private SensitiveDataRedactor() {
	}

	public static String redactHeaderValue(String name, String value) {
		if (value == null) {
			return null;
		}
		if (name != null && SENSITIVE_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
			return REDACTED;
		}
		return value;
	}

	public static Map<String, Object> redactHeaders(Map<String, ?> headers) {
		Map<String, Object> out = new LinkedHashMap<>();
		if (headers == null) {
			return out;
		}
		for (Map.Entry<String, ?> e : headers.entrySet()) {
			String key = e.getKey();
			Object value = e.getValue();
			if (value == null) {
				out.put(key, null);
			} else {
				out.put(key, redactHeaderValue(key, String.valueOf(value)));
			}
		}
		return out;
	}

	public static String redactQuery(String query) {
		if (query == null || query.isEmpty()) {
			return query;
		}
		StringBuilder sb = new StringBuilder();
		String[] pairs = query.split("&");
		for (int i = 0; i < pairs.length; i++) {
			if (i > 0) {
				sb.append('&');
			}
			String pair = pairs[i];
			int eq = pair.indexOf('=');
			String rawKey = eq >= 0 ? pair.substring(0, eq) : pair;
			String decodedKey = urlDecode(rawKey);
			if (isAccessTokenKey(decodedKey) || isAccessTokenKey(rawKey)) {
				sb.append(rawKey).append('=').append(REDACTED);
			} else {
				sb.append(pair);
			}
		}
		return sb.toString();
	}

	public static String redactUrl(String url) {
		if (url == null || url.isEmpty()) {
			return url;
		}
		int q = url.indexOf('?');
		if (q < 0) {
			return url;
		}
		int hash = url.indexOf('#', q);
		String base = url.substring(0, q + 1);
		String query;
		String suffix;
		if (hash >= 0) {
			query = url.substring(q + 1, hash);
			suffix = url.substring(hash);
		} else {
			query = url.substring(q + 1);
			suffix = "";
		}
		return base + redactQuery(query) + suffix;
	}

	/**
	 * Recursively redacts known secret keys. Returns {@code null} when the payload is not JSON
	 * so callers can log length/content-type instead of the raw body.
	 */
	public static String redactJsonOrNull(String json) {
		if (json == null || json.isBlank()) {
			return json;
		}
		try {
			JsonNode root = MAPPER.readTree(json);
			redactNode(root);
			return MAPPER.writeValueAsString(root);
		} catch (Exception e) {
			return null;
		}
	}

	private static void redactNode(JsonNode node) {
		if (node == null) {
			return;
		}
		if (node.isObject()) {
			ObjectNode obj = (ObjectNode) node;
			Iterator<String> names = obj.fieldNames();
			java.util.List<String> keys = new java.util.ArrayList<>();
			while (names.hasNext()) {
				keys.add(names.next());
			}
			for (String key : keys) {
				if (SENSITIVE_JSON_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
					obj.put(key, REDACTED);
				} else {
					redactNode(obj.get(key));
				}
			}
		} else if (node.isArray()) {
			for (JsonNode child : node) {
				redactNode(child);
			}
		}
	}

	public static boolean isAccessTokenKey(String key) {
		return key != null && "access_token".equalsIgnoreCase(key);
	}

	private static String urlDecode(String raw) {
		try {
			return URLDecoder.decode(raw, StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			return raw;
		}
	}
}
