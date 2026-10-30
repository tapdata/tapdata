package com.tapdata.tm.audit;

import com.tapdata.tm.base.security.SensitiveDataRedactor;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 审计内容脱敏：秘密字段不记录值，只记录设置、替换、清除或未变更；自由文本移除连接串、凭据参数和长令牌（需求 12.4）。
 */
public final class AuditRedactor {
	public static final String MASK = "******";
	private static final int MAX_TEXT = 500;
	private static final Set<String> SECRET_KEY_PARTS = Set.of(
			"password", "passwd", "pwd", "secret", "token", "accesscode", "access_code", "accesskey", "secretkey",
			"privatekey", "private_key", "credential", "cookie", "certificate", "sslkey", "sslcert", "keystore",
			"truststore", "authorization", "clientsecret", "client_secret", "apikey", "api_key", "uri", "connectionstring",
			"verifycode", "validatecode");
	private static final Pattern URI_WITH_USERINFO = Pattern.compile("([a-zA-Z][a-zA-Z0-9+.-]*://)[^\\s/@]+@");
	private static final Pattern JDBC_URL = Pattern.compile("jdbc:[a-zA-Z0-9]+:[^\\s,;)\"']+");
	private static final Pattern KEY_VALUE_SECRET = Pattern.compile(
			"(?i)((?:password|passwd|pwd|secret|token|access_?code|access_?key|secret_?key|api_?key)\\s*[=:]\\s*)[^\\s,;&\"'}]+");
	private static final Pattern PEM_BLOCK = Pattern.compile("-----BEGIN [A-Z ]+-----[\\s\\S]*?-----END [A-Z ]+-----");
	private static final Pattern LONG_TOKEN = Pattern.compile("\\b[A-Za-z0-9_\\-]{40,}\\b");

	private AuditRedactor() {
	}

	public static boolean isSecretKey(String key) {
		if (key == null) {
			return false;
		}
		String normalized = key.toLowerCase(Locale.ROOT).replace("-", "").replace(".", "");
		if (SensitiveDataRedactor.isAccessTokenKey(key)) {
			return true;
		}
		for (String part : SECRET_KEY_PARTS) {
			if (normalized.contains(part.replace("_", ""))) {
				return true;
			}
		}
		return false;
	}

	/** 根据前后值得到秘密字段的变更状态，不保留值本身。 */
	public static String secretState(Object before, Object after) {
		boolean hadValue = before != null && !String.valueOf(before).isEmpty();
		boolean hasValue = after != null && !String.valueOf(after).isEmpty();
		if (!hadValue && hasValue) {
			return AuditValues.SECRET_SET;
		}
		if (hadValue && !hasValue) {
			return AuditValues.SECRET_CLEARED;
		}
		if (hadValue && !Objects.equals(String.valueOf(before), String.valueOf(after))) {
			return AuditValues.SECRET_REPLACED;
		}
		return AuditValues.SECRET_UNCHANGED;
	}

	/** 生成字段变更：秘密字段只给状态，普通字段给安全化的前后值；值相同时返回 null。 */
	public static AuditChange change(String field, Object before, Object after) {
		if (isSecretKey(field)) {
			String state = secretState(before, after);
			return AuditValues.SECRET_UNCHANGED.equals(state) ? null : AuditChange.secret(field, state);
		}
		String b = before == null ? null : text(String.valueOf(before));
		String a = after == null ? null : text(String.valueOf(after));
		return Objects.equals(b, a) ? null : new AuditChange(field, b, a, null);
	}

	/** 自由文本（失败原因、对象名、摘要）脱敏并截断。 */
	public static String text(String value) {
		if (value == null) {
			return null;
		}
		String result = PEM_BLOCK.matcher(value).replaceAll(MASK);
		result = URI_WITH_USERINFO.matcher(result).replaceAll("$1" + MASK + "@");
		result = JDBC_URL.matcher(result).replaceAll("jdbc:" + MASK);
		result = KEY_VALUE_SECRET.matcher(result).replaceAll("$1" + MASK);
		result = SensitiveDataRedactor.redactAccessTokenInText(result);
		result = LONG_TOKEN.matcher(result).replaceAll(MASK);
		if (result.length() > MAX_TEXT) {
			result = result.substring(0, MAX_TEXT) + "...";
		}
		return result;
	}

	/** 失败登录的尝试账号：保留首尾各一个字符与邮箱域名。 */
	public static String maskAccount(String account) {
		if (account == null || account.isBlank()) {
			return null;
		}
		String value = account.trim();
		String domain = "";
		int at = value.indexOf('@');
		if (at > 0) {
			domain = value.substring(at);
			value = value.substring(0, at);
		}
		String masked = value.length() <= 2 ? value.charAt(0) + "*"
				: value.charAt(0) + "*".repeat(Math.min(value.length() - 2, 6)) + value.charAt(value.length() - 1);
		return text(masked + domain);
	}

	/** API 凭据的安全化标识：只保留末 4 位。 */
	public static String credentialRef(String credential) {
		if (credential == null || credential.length() < 8) {
			return null;
		}
		return "***" + credential.substring(credential.length() - 4);
	}
}
