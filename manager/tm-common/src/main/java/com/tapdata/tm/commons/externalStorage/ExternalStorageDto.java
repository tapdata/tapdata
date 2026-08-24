package com.tapdata.tm.commons.externalStorage;

import com.mongodb.ConnectionString;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.bean.ResponseBody;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;


/**
 * External Storage
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ExternalStorageDto extends BaseDto {
	public static final String MASK_PWD = "******";
	private String name;
	private String type;
	private String uri;
	private String table;
	private Integer ttlDay;
	private Boolean canEdit = true;
	private Boolean canDelete;
	private boolean defaultStorage = false;
	private List<String> baseURLs;
	private String accessToken;
	private Integer connectTimeoutMs;
	private Integer readTimeoutMs;

	private boolean ssl;
	private String sslCA;
	private String sslKey;
	private String sslPass;
	private boolean sslValidate;
	private boolean checkServerIdentity;

	private Integer inMemSize;
	private String maxSizePolicy;
	private Integer writeDelaySeconds;
	private String status;
	private Map<String, String> attrs;
	/**
	 * 测试响应消息
	 */
	private ResponseBody response_body;

	public String maskUriPassword() {
		if (isMongoDBUri() && StringUtils.isNotBlank(uri)) {
			try {
				ConnectionString connectionString = new ConnectionString(uri);
				char[] passwordChars = connectionString.getPassword();
				if (null != passwordChars && passwordChars.length > 0) {
					StringBuilder password = new StringBuilder();
					for (char passwordChar : passwordChars) {
						password.append(passwordChar);
					}
					String username = connectionString.getUsername();
					if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
						String maskedUri = uri.replace(username + ":" + password, username + ":" + MASK_PWD).
								replace(username + ":" + URLEncoder.encode(password.toString(), "UTF-8"), username + ":" + MASK_PWD);
						if (!StringUtils.equals(uri, maskedUri)) {
							return maskedUri;
						}
					}
				}
			} catch (Exception ignored) {
			}
			return maskUserInfoPassword();
		}
		return uri;
	}

	private boolean isMongoDBUri() {
		return ExternalStorageType.mongodb.name().equals(type)
				|| StringUtils.startsWithIgnoreCase(uri, "mongodb://")
				|| StringUtils.startsWithIgnoreCase(uri, "mongodb+srv://");
	}

	private String maskUserInfoPassword() {
		int schemeIndex = StringUtils.indexOf(uri, "://");
		if (schemeIndex < 0) {
			return uri;
		}
		int userInfoStart = schemeIndex + 3;
		int userInfoEnd = StringUtils.indexOf(uri, '@', userInfoStart);
		if (userInfoEnd < 0) {
			return uri;
		}
		int pathStart = findFirstPathSeparator(userInfoStart);
		if (pathStart >= 0 && pathStart < userInfoEnd) {
			return uri;
		}
		int passwordStart = StringUtils.indexOf(uri, ':', userInfoStart);
		if (passwordStart < 0 || passwordStart > userInfoEnd) {
			return uri;
		}
		return uri.substring(0, passwordStart + 1) + MASK_PWD + uri.substring(userInfoEnd);
	}

	private int findFirstPathSeparator(int start) {
		int pathIndex = StringUtils.indexOf(uri, '/', start);
		int queryIndex = StringUtils.indexOf(uri, '?', start);
		int fragmentIndex = StringUtils.indexOf(uri, '#', start);
		int result = -1;
		if (pathIndex >= 0) {
			result = pathIndex;
		}
		if (queryIndex >= 0 && (result < 0 || queryIndex < result)) {
			result = queryIndex;
		}
		if (fragmentIndex >= 0 && (result < 0 || fragmentIndex < result)) {
			result = fragmentIndex;
		}
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ExternalStorageDto.class.getSimpleName() + "[", "]")
				.add("name='" + name + "'")
				.add("type='" + type + "'")
				.add("uri='" + maskUriPassword() + "'")
				.add("table='" + table + "'")
				.add("ttlDay=" + ttlDay)
				.toString();
	}
}
