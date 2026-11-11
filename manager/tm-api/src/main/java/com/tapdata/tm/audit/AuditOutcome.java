package com.tapdata.tm.audit;

/** 操作结果；ACCEPTED 仅用于异步请求受理，PARTIAL 仅用于批量混合结果 */
public enum AuditOutcome {
	SUCCESS, FAILED, ACCEPTED, PARTIAL
}
