package com.tapdata.tm.externalStorage.vo;

import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import lombok.Data;

/**
 * @author samuel
 * @Description
 * @create 2023-02-09 14:29
 **/
@Data
public class ExternalStorageVo {
	private String name;
	private String type;
	private String uri;
	private String table;

	public void setUri(String uri) {
		if (uri == null) {
			this.uri = null;
			return;
		}
		ExternalStorageDto externalStorageDto = new ExternalStorageDto();
		externalStorageDto.setType(type);
		externalStorageDto.setUri(uri);
		this.uri = externalStorageDto.maskUriPassword();
	}
}
