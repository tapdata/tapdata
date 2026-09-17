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
			"client_secret",
			"password",
			"passwd",
			"secret",
			"token",
			"privatekey",
			"accesskey",
			"secretkey"
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
		// Referer / Location / Link / Origin can embed ?access_token= (TAP-11883 D-06).
		return redactAccessTokenInText(value);
	}

	/**
	 * Replace {@code access_token=} query/hash assignments inside a URL or
	 * URL-bearing string. Leaves other params (including {@code login_code}) intact.
	 */
	public static String redactAccessTokenInText(String value) {
		if (value == null || value.isEmpty()) {
			return value;
		}
		final String needle = "access_token=";
		int from = 0;
		StringBuilder sb = null;
		while (true) {
			int idx = indexOfIgnoreCase(value, needle, from);
			if (idx < 0) {
				if (sb == null) {
					return value;
				}
				sb.append(value, from, value.length());
				return sb.toString();
			}
			if (idx > 0) {
				char prev = value.charAt(idx - 1);
				if (prev != '?' && prev != '&' && prev != '#') {
					if (sb == null) {
						sb = new StringBuilder(value.length());
					}
					int keyEnd = idx + needle.length();
					sb.append(value, from, keyEnd);
					from = keyEnd;
					continue;
				}
			}
			if (sb == null) {
				sb = new StringBuilder(value.length());
			}
			int eq = idx + "access_token".length();
			sb.append(value, from, eq + 1);
			sb.append(REDACTED);
			from = skipQueryValue(value, eq + 1);
		}
	}

	private static int indexOfIgnoreCase(String value, String needle, int from) {
		int max = value.length() - needle.length();
		for (int i = from; i <= max; i++) {
			if (value.regionMatches(true, i, needle, 0, needle.length())) {
				return i;
			}
		}
		return -1;
	}

	private static int skipQueryValue(String value, int start) {
		int i = start;
		while (i < value.length()) {
			char c = value.charAt(i);
			if (c == '&' || c == '#' || c == ';' || c == '>' || Character.isWhitespace(c)) {
				break;
			}
			i++;
		}
		return i;
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
