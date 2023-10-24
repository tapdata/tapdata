package com.tapdata.tm.shareCdcTableMapping;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;


/**
 * shareCdcTableMapping
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Data
public class ShareCdcTableMappingDto extends BaseDto {

	public static final String VERSION_V1 = "v1";
	public static final String VERSION_V2 = "v2";

	private String sign;
	private String version = VERSION_V1;
	private String tableName;
	private String externalStorageTableName;
	private String shareCdcTaskId;
	private String connectionId;

	public String genSign() {
		if (StringUtils.isBlank(connectionId)) throw new IllegalArgumentException("Connection id cannot be blank");
		if (StringUtils.isBlank(tableName)) throw new IllegalArgumentException("Table name cannot be blank");
		return String.join("_", connectionId, tableName);
	}

	public static String genSign(String connId, String tableName) {
		return String.join("_", connId, tableName);
	}
}