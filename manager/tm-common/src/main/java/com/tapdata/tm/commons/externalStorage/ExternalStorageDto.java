package com.tapdata.tm.commons.externalStorage;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


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
	private String baseUrl;
	private String accessToken;
	private Integer connectTimeoutMs;
	private Integer readTimeoutMs;
	@Override
	public String toString() {
		return "ExternalStorage{" +
				"name='" + name + '\'' +
				", type='" + type + '\'' +
				", uri='" + uri + '\'' +
				(type.equals("mongodb") ? (", table='" + table + '\'') : "") +
				"} ";
	}
}