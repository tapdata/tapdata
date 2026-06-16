package com.tapdata.tm.task.param;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
public class TableLogCollectorParam {

	private String connectionId;

	private Set<String> tableNames;

	private Map<String, Object> nodeConfig;

	public TableLogCollectorParam(String connectionId, Set<String> tableNames) {
		this.connectionId = connectionId;
		this.tableNames = tableNames;
	}
}
