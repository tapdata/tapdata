package com.tapdata.tm.audit.query;

import lombok.Data;

/** 审计查询条件（需求 13.4）；所有条件可组合，keyword 在来源 IP、Action、对象、事件标识中模糊匹配。 */
@Data
public class AuditQuery {
	private Long start;
	private Long end;
	private String eventType;
	private String user;
	private String source;
	private String sourceIp;
	private String action;
	private String object;
	private String outcome;
	private String eventId;
	private String operationId;
	private String keyword;
	private int page = 1;
	private int size = 20;
}
