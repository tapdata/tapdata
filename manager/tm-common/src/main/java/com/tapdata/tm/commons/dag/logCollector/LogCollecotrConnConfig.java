package com.tapdata.tm.commons.dag.logCollector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogCollecotrConnConfig {
    private String connectionId;
    private List<String> tableNames;
		private List<String> exclusionTables;

	public LogCollecotrConnConfig(String connectionId, List<String> tableNames) {
		this.connectionId = connectionId;
		this.tableNames = tableNames;
	}
}