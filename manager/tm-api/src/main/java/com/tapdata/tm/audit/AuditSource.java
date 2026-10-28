package com.tapdata.tm.audit;

/** 操作来源：TapData UI、OpenAPI 调用身份或无真实用户参与的系统事件 */
public enum AuditSource {
	UI, OPENAPI, SYSTEM
}
