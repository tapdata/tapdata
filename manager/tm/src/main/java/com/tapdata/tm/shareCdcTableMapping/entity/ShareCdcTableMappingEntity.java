package com.tapdata.tm.shareCdcTableMapping.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * shareCdcTableMapping
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ShareCdcTableMapping")
public class ShareCdcTableMappingEntity extends BaseEntity {
	private String sign;
	private String version;
	private String tableName;
	private String externalStorageTableName;
	private String shareCdcTaskId;
	private String connectionId;

	public String genSign() {
		if (StringUtils.isBlank(shareCdcTaskId))
			throw new IllegalArgumentException("Share cdc task id cannot be blank");
		if (StringUtils.isBlank(connectionId)) throw new IllegalArgumentException("Connection id cannot be blank");
		if (StringUtils.isBlank(tableName)) throw new IllegalArgumentException("Table name cannot be blank");
		return String.join("_", shareCdcTaskId, connectionId, tableName);
	}
}