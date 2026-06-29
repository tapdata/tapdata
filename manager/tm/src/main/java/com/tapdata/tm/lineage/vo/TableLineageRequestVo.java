package com.tapdata.tm.lineage.vo;

import com.tapdata.tm.lineage.entity.LineageType;
import com.tapdata.tm.lineage.util.LineageTypeUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author samuel
 * @Description
 * @create 2023-05-19 12:10
 **/
@Data
public class TableLineageRequestVo {

	private String connectionId;
	private String table;
	private String type;
	private LineageType lineageType;

	public TableLineageRequestVo(String connectionId, String table, String type) {
		this.connectionId = connectionId;
		this.table = table;
		this.type = type;
		this.lineageType = LineageTypeUtil.initLineageType(type);
	}

	public boolean isEmpty() {
		return StringUtils.isBlank(connectionId) || StringUtils.isBlank(table);
	}
}
