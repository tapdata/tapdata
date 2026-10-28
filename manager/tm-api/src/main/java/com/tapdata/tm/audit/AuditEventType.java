package com.tapdata.tm.audit;

/** 稳定的审计事件分类，页面按此分类筛选，文案取 audit_event_type_<小写名>。 */
public enum AuditEventType {
	AUTH,
	ACCESS_CONTROL,
	USER,
	ROLE,
	DATASOURCE,
	EXTERNAL_STORAGE,
	TASK,
	INSPECT,
	LOG_CACHE,
	LIVE_CACHE,
	DATA_API,
	SETTINGS,
	NOTIFICATION,
	LICENSE,
	CONNECTOR,
	CLUSTER,
	SERVICE,
	AUDIT,
	MESSAGE
}
