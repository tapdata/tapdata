package com.tapdata.tm.lineage.vo;

import com.tapdata.tm.lineage.entity.LineageType;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author samuel
 * @Description
 * @create 2023-05-19 12:10
 **/
@Data
public class TableLineageRequestVo {

	public static final LineageType DEFAULT_LINEAGE_TYPE = LineageType.ALL_STREAM;

	private String connectionId;
	private String table;
	private String type;
	private LineageType lineageType;

	public TableLineageRequestVo(String connectionId, String table, String type) {
		this.connectionId = connectionId;
		this.table = table;
		this.type = type;
		this.lineageType = initLineageType(type);
	}

	private LineageType initLineageType(String type) {
		if (StringUtils.isBlank(type)) {
			return DEFAULT_LINEAGE_TYPE;
		}
		LineageType lineageType = LineageType.fromType(type);
		if (null == lineageType) {
			lineageType = DEFAULT_LINEAGE_TYPE;
		}
		return lineageType;
	}

	public boolean isEmpty() {
		return StringUtils.isBlank(connectionId) || StringUtils.isBlank(table);
	}
}
