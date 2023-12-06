package com.tapdata.tm.commons.externalStorage;

import com.mongodb.ConnectionString;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.bean.ResponseBody;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
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
	/**
	 * 测试响应消息
	 */
	private ResponseBody response_body;

	public String maskUriPassword() {
		if (ExternalStorageType.mongodb.name().equals(type) && StringUtils.isNotBlank(uri)) {
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
						return uri.replace(username + ":" + password, username + ":" + MASK_PWD);
					}
				}
			} catch (Exception ignored) {
			}
		}
		return uri;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ExternalStorageDto.class.getSimpleName() + "[", "]")
				.add("name='" + name + "'")
				.add("type='" + type + "'")
				.add("uri='" + uri + "'")
				.add("table='" + table + "'")
				.add("ttlDay=" + ttlDay)
				.toString();
	}
}
