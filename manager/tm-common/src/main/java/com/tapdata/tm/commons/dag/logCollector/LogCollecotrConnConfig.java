package com.tapdata.tm.commons.dag.logCollector;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
public class LogCollecotrConnConfig implements Serializable {
    private String connectionId;
    private List<String> namespace;
    private List<String> tableNames;
	private List<String> exclusionTables;

	public LogCollecotrConnConfig(String connectionId, List<String> tableNames) {
		this.connectionId = connectionId;
		this.tableNames = tableNames;
	}

	public LogCollecotrConnConfig(String connectionId, List<String> tableNames, List<String> exclusionTables) {
		this.connectionId = connectionId;
		this.exclusionTables = exclusionTables;
		this.tableNames = tableNames;
	}
}
