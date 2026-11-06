package com.tapdata.tm.audit;

/** SINGLE 为同步操作单事件；异步操作由 REQUEST 与 FINAL 两条事件组成，共用 Operation ID */
public enum AuditPhase {
	SINGLE, REQUEST, FINAL
}
