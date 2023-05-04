package com.tapdata.tm.commons.externalStorage;

import com.mongodb.ConnectionString;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;


/**
 * External Storage
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ExternalStorageDto extends BaseDto {
	private String name;
	private String type;
	private String uri;
	private String table;
	private Integer ttlDay;
	private boolean canEdit = false;
	private boolean canDelete = true;
	private boolean defaultStorage = false;
	private List<String> baseURLs;
	private String accessToken;
	private Integer connectTimeoutMs;
	private Integer readTimeoutMs;

	public String maskUriPassword() {
		if (ExternalStorageType.mongodb.name().equals(type) && StringUtils.isNotBlank(uri)) {

			ConnectionString connectionString = new ConnectionString(uri);
			char[] passwordChars = connectionString.getPassword();
			if (null != passwordChars && passwordChars.length > 0) {
				StringBuilder password = new StringBuilder();
				for (char passwordChar : passwordChars) {
					password.append(passwordChar);
				}
				String username = connectionString.getUsername();
				if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
					return uri = uri.replace(username + ":" + password, username + ":******");
				}
			}
		}
		return uri;
	}

	@Override
	public String toString() {
		return "ExternalStorage{" +
				"name='" + name + '\'' +
				", type='" + type + '\'' +
				", uri='" + maskUriPassword() + '\'' +
				(type.equals("mongodb") ? (", table='" + table + '\'') : "") +
				"} ";
	}
}