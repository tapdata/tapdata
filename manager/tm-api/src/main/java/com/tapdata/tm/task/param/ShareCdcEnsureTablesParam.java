package com.tapdata.tm.task.param;

import lombok.Data;

import java.util.List;

@Data
public class ShareCdcEnsureTablesParam {

	private String syncTaskId;
	private String connectionId;
	private String nodeId;
	private List<String> tableNames;
	private Boolean waitReady;
}
