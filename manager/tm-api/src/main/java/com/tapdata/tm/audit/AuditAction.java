package com.tapdata.tm.audit;

import static com.tapdata.tm.audit.AuditDetailLevel.BASIC;
import static com.tapdata.tm.audit.AuditDetailLevel.DIFF;
import static com.tapdata.tm.audit.AuditDetailLevel.SUMMARY;

/**
 * 审计事件目录：Action 是稳定的业务动作，不随按钮、URL 或接口变化；同一动作来自 UI 或 OpenAPI 时使用同一枚举值。
 * 页面文案取 audit_action_&lt;key 中 . 替换为 _&gt;，三语文案由 AuditCatalogCoverageTest 校验。
 * 新增会改变配置、资源、权限、安全状态、任务状态、数据或外部系统的产品操作，必须先在这里登记。
 */
public enum AuditAction {
	// 认证与会话
	LOGIN("auth.login", AuditEventType.AUTH, "session", false, BASIC),
	LOGOUT("auth.logout", AuditEventType.AUTH, "session", false, BASIC),
	SESSION_TERMINATE("auth.session_terminate", AuditEventType.AUTH, "session", false, BASIC),
	API_AUTHENTICATE("auth.api_authenticate", AuditEventType.AUTH, "credential", false, BASIC),
	ACCESS_DENIED("access.denied", AuditEventType.ACCESS_CONTROL, "resource", false, BASIC),

	// 用户、角色与权限
	USER_CREATE("user.create", AuditEventType.USER, "user", false, BASIC),
	USER_UPDATE("user.update", AuditEventType.USER, "user", false, SUMMARY),
	USER_ENABLE("user.enable", AuditEventType.USER, "user", false, BASIC),
	USER_DISABLE("user.disable", AuditEventType.USER, "user", false, BASIC),
	USER_DELETE("user.delete", AuditEventType.USER, "user", false, BASIC),
	USER_CHANGE_PASSWORD("user.change_password", AuditEventType.USER, "user", false, BASIC),
	USER_RESET_PASSWORD("user.reset_password", AuditEventType.USER, "user", false, BASIC),
	USER_RESET_ACCESS_CODE("user.reset_access_code", AuditEventType.USER, "user", false, BASIC),
	USER_VIEW_ACCESS_CODE("user.view_access_code", AuditEventType.USER, "credential", false, BASIC),
	USER_UPDATE_CONTACT("user.update_contact", AuditEventType.USER, "user", false, BASIC),
	USER_UPDATE_TAGS("user.update_tags", AuditEventType.USER, "user", false, BASIC),
	USER_IMPORT("user.import", AuditEventType.USER, "user", false, SUMMARY),
	USER_GROUP_CREATE("user_group.create", AuditEventType.USER, "user_group", false, BASIC),
	USER_GROUP_UPDATE("user_group.update", AuditEventType.USER, "user_group", false, SUMMARY),
	USER_GROUP_DELETE("user_group.delete", AuditEventType.USER, "user_group", false, BASIC),
	ROLE_CREATE("role.create", AuditEventType.ROLE, "role", false, BASIC),
	ROLE_UPDATE("role.update", AuditEventType.ROLE, "role", false, SUMMARY),
	ROLE_DELETE("role.delete", AuditEventType.ROLE, "role", false, BASIC),
	ROLE_GRANT("role.grant", AuditEventType.ROLE, "user", false, SUMMARY),
	ROLE_REVOKE("role.revoke", AuditEventType.ROLE, "user", false, SUMMARY),
	PERMISSION_UPDATE("permission.update", AuditEventType.ROLE, "role", false, SUMMARY),
	DATA_PERMISSION_UPDATE("permission.data_update", AuditEventType.ROLE, "resource", false, SUMMARY),

	// 数据源与外部存储
	DATASOURCE_CREATE("datasource.create", AuditEventType.DATASOURCE, "datasource", false, BASIC),
	DATASOURCE_UPDATE("datasource.update", AuditEventType.DATASOURCE, "datasource", false, SUMMARY),
	DATASOURCE_COPY("datasource.copy", AuditEventType.DATASOURCE, "datasource", false, BASIC),
	DATASOURCE_DELETE("datasource.delete", AuditEventType.DATASOURCE, "datasource", false, BASIC),
	DATASOURCE_IMPORT("datasource.import", AuditEventType.DATASOURCE, "datasource", false, SUMMARY),
	DATASOURCE_EXPORT("datasource.export", AuditEventType.DATASOURCE, "datasource", false, BASIC),
	DATASOURCE_TEST("datasource.test", AuditEventType.DATASOURCE, "datasource", false, BASIC),
	DATASOURCE_UPDATE_TAGS("datasource.update_tags", AuditEventType.DATASOURCE, "datasource", false, BASIC),
	DATASOURCE_UPDATE_AGENT("datasource.update_agent", AuditEventType.DATASOURCE, "datasource", false, SUMMARY),
	EXTERNAL_STORAGE_CREATE("external_storage.create", AuditEventType.EXTERNAL_STORAGE, "external_storage", false, BASIC),
	EXTERNAL_STORAGE_UPDATE("external_storage.update", AuditEventType.EXTERNAL_STORAGE, "external_storage", false, SUMMARY),
	EXTERNAL_STORAGE_DELETE("external_storage.delete", AuditEventType.EXTERNAL_STORAGE, "external_storage", false, BASIC),
	EXTERNAL_STORAGE_TEST("external_storage.test", AuditEventType.EXTERNAL_STORAGE, "external_storage", false, BASIC),
	EXTERNAL_STORAGE_SET_DEFAULT("external_storage.set_default", AuditEventType.EXTERNAL_STORAGE, "external_storage", false, BASIC),

	// 任务
	TASK_CREATE("task.create", AuditEventType.TASK, "task", false, BASIC),
	TASK_UPDATE("task.update", AuditEventType.TASK, "task", false, BASIC),
	TASK_RENAME("task.rename", AuditEventType.TASK, "task", false, DIFF),
	TASK_COPY("task.copy", AuditEventType.TASK, "task", false, BASIC),
	TASK_DELETE("task.delete", AuditEventType.TASK, "task", true, BASIC),
	TASK_START("task.start", AuditEventType.TASK, "task", true, BASIC),
	TASK_STOP("task.stop", AuditEventType.TASK, "task", true, BASIC),
	TASK_FORCE_STOP("task.force_stop", AuditEventType.TASK, "task", true, BASIC),
	TASK_RESET("task.reset", AuditEventType.TASK, "task", true, BASIC),
	TASK_IMPORT("task.import", AuditEventType.TASK, "task", false, SUMMARY),
	TASK_EXPORT("task.export", AuditEventType.TASK, "task", false, BASIC),
	TASK_UPDATE_TAGS("task.update_tags", AuditEventType.TASK, "task", false, BASIC),
	TASK_UPDATE_ALARM("task.update_alarm", AuditEventType.TASK, "task", false, SUMMARY),
	TASK_DOWNLOAD_LOG("task.download_log", AuditEventType.TASK, "task", false, BASIC),
	TASK_TEST_RUN("task.test_run", AuditEventType.TASK, "task", false, BASIC),
	DATA_PREVIEW("data.preview", AuditEventType.TASK, "table", false, BASIC),

	// 数据校验
	INSPECT_CREATE("inspect.create", AuditEventType.INSPECT, "inspect", false, BASIC),
	INSPECT_UPDATE("inspect.update", AuditEventType.INSPECT, "inspect", false, BASIC),
	INSPECT_DELETE("inspect.delete", AuditEventType.INSPECT, "inspect", false, BASIC),
	INSPECT_START("inspect.start", AuditEventType.INSPECT, "inspect", true, BASIC),
	INSPECT_STOP("inspect.stop", AuditEventType.INSPECT, "inspect", true, BASIC),
	INSPECT_IMPORT("inspect.import", AuditEventType.INSPECT, "inspect", false, SUMMARY),
	INSPECT_REPAIR("inspect.repair", AuditEventType.INSPECT, "inspect", false, BASIC),
	INSPECT_EXPORT_RESULT("inspect.export_result", AuditEventType.INSPECT, "inspect", false, BASIC),
	INSPECT_UPDATE_CONFIG("inspect.update_config", AuditEventType.INSPECT, "task", false, BASIC),
	INSPECT_DELETE_RESULT("inspect.delete_result", AuditEventType.INSPECT, "inspect", false, BASIC),

	// CDC Log Cache 与 Live Cache
	LOG_CACHE_UPDATE("log_cache.update", AuditEventType.LOG_CACHE, "log_cache", false, BASIC),
	LOG_CACHE_UPDATE_CONFIG("log_cache.update_config", AuditEventType.LOG_CACHE, "log_cache", false, DIFF),
	LOG_CACHE_CREATE("log_cache.create", AuditEventType.LOG_CACHE, "log_cache", false, BASIC),
	LOG_CACHE_STOP_TABLES("log_cache.stop_tables", AuditEventType.LOG_CACHE, "log_cache", false, SUMMARY),
	LOG_CACHE_RESUME_TABLES("log_cache.resume_tables", AuditEventType.LOG_CACHE, "log_cache", false, SUMMARY),
	LOG_CACHE_CANCEL_MERGE("log_cache.cancel_merge", AuditEventType.LOG_CACHE, "log_cache", false, BASIC),
	LIVE_CACHE_CREATE("live_cache.create", AuditEventType.LIVE_CACHE, "live_cache", false, BASIC),
	LIVE_CACHE_UPDATE("live_cache.update", AuditEventType.LIVE_CACHE, "live_cache", false, BASIC),
	LIVE_CACHE_DELETE("live_cache.delete", AuditEventType.LIVE_CACHE, "live_cache", true, BASIC),
	LIVE_CACHE_COPY("live_cache.copy", AuditEventType.LIVE_CACHE, "live_cache", false, BASIC),

	// 数据 API
	API_APP_CREATE("api_app.create", AuditEventType.DATA_API, "api_app", false, BASIC),
	API_APP_UPDATE("api_app.update", AuditEventType.DATA_API, "api_app", false, BASIC),
	API_APP_DELETE("api_app.delete", AuditEventType.DATA_API, "api_app", false, BASIC),
	API_CREATE("api.create", AuditEventType.DATA_API, "api", false, BASIC),
	API_UPDATE("api.update", AuditEventType.DATA_API, "api", false, BASIC),
	API_COPY("api.copy", AuditEventType.DATA_API, "api", false, BASIC),
	API_DELETE("api.delete", AuditEventType.DATA_API, "api", false, BASIC),
	API_PUBLISH("api.publish", AuditEventType.DATA_API, "api", true, BASIC),
	API_UNPUBLISH("api.unpublish", AuditEventType.DATA_API, "api", true, BASIC),
	API_IMPORT("api.import", AuditEventType.DATA_API, "api", false, SUMMARY),
	API_EXPORT("api.export", AuditEventType.DATA_API, "api", false, BASIC),
	API_UPDATE_SECURITY("api.update_security", AuditEventType.DATA_API, "api", false, SUMMARY),
	API_DEBUG("api.debug", AuditEventType.DATA_API, "api", false, BASIC),
	API_EXPORT_DOC("api.export_doc", AuditEventType.DATA_API, "api", false, BASIC),
	API_CLIENT_CREATE("api_client.create", AuditEventType.DATA_API, "api_client", false, BASIC),
	API_CLIENT_UPDATE("api_client.update", AuditEventType.DATA_API, "api_client", false, SUMMARY),
	API_CLIENT_DELETE("api_client.delete", AuditEventType.DATA_API, "api_client", false, BASIC),
	API_CLIENT_VIEW_SECRET("api_client.view_secret", AuditEventType.DATA_API, "credential", false, BASIC),

	// 系统设置、通知、License、连接器
	SETTINGS_UPDATE("settings.update", AuditEventType.SETTINGS, "settings", false, DIFF),
	SETTINGS_TEST("settings.test", AuditEventType.SETTINGS, "settings", false, BASIC),
	AUDIT_RETENTION_UPDATE("audit.retention_update", AuditEventType.AUDIT, "settings", false, DIFF),
	SSO_CONFIG_UPDATE("sso.update", AuditEventType.SETTINGS, "sso_config", false, SUMMARY),
	SSO_CONFIG_TEST("sso.test", AuditEventType.SETTINGS, "sso_config", false, BASIC),
	LDAP_TEST("ldap.test", AuditEventType.SETTINGS, "ldap_config", false, BASIC),
	ALARM_RULE_UPDATE("alarm.rule_update", AuditEventType.NOTIFICATION, "alarm_rule", false, BASIC),
	ALARM_RECEIVER_UPDATE("alarm.receiver_update", AuditEventType.NOTIFICATION, "alarm_rule", false, SUMMARY),
	ALARM_CLOSE("alarm.close", AuditEventType.NOTIFICATION, "alarm", false, BASIC),
	WEBHOOK_CREATE("webhook.create", AuditEventType.NOTIFICATION, "webhook", false, BASIC),
	WEBHOOK_UPDATE("webhook.update", AuditEventType.NOTIFICATION, "webhook", false, BASIC),
	WEBHOOK_ENABLE("webhook.enable", AuditEventType.NOTIFICATION, "webhook", false, BASIC),
	WEBHOOK_DISABLE("webhook.disable", AuditEventType.NOTIFICATION, "webhook", false, BASIC),
	WEBHOOK_DELETE("webhook.delete", AuditEventType.NOTIFICATION, "webhook", false, BASIC),
	WEBHOOK_TEST("webhook.test", AuditEventType.NOTIFICATION, "webhook", false, BASIC),
	NOTIFICATION_UPDATE("notification.update", AuditEventType.NOTIFICATION, "notification", false, BASIC),
	LICENSE_IMPORT("license.import", AuditEventType.LICENSE, "license", false, BASIC),
	CONNECTOR_INSTALL("connector.install", AuditEventType.CONNECTOR, "connector", false, BASIC),
	CONNECTOR_UPDATE("connector.update", AuditEventType.CONNECTOR, "connector", false, BASIC),
	CONNECTOR_DELETE("connector.delete", AuditEventType.CONNECTOR, "connector", false, BASIC),
	CUSTOMER_UPDATE("customer.update", AuditEventType.SETTINGS, "customer", false, BASIC),
	MESSAGE_READ("message.read", AuditEventType.MESSAGE, "message", false, BASIC),
	MESSAGE_DELETE("message.delete", AuditEventType.MESSAGE, "message", false, BASIC),

	// 集群、Agent 与服务生命周期
	SERVICE_START("service.start", AuditEventType.SERVICE, "service", true, BASIC),
	SERVICE_STOP("service.stop", AuditEventType.SERVICE, "service", true, BASIC),
	SERVICE_RESTART("service.restart", AuditEventType.SERVICE, "service", true, BASIC),
	SERVICE_UPGRADE("service.upgrade", AuditEventType.SERVICE, "service", true, BASIC),
	SERVICE_START_FAILED("service.start_failed", AuditEventType.SERVICE, "service", false, BASIC),
	SERVICE_ABNORMAL_STOP("service.abnormal_stop", AuditEventType.SERVICE, "service", false, BASIC),
	SERVICE_DISCONNECT("service.disconnect", AuditEventType.SERVICE, "service", false, BASIC),
	SERVICE_RECONNECT("service.reconnect", AuditEventType.SERVICE, "service", false, BASIC),
	AGENT_CREATE("agent.create", AuditEventType.CLUSTER, "agent", false, BASIC),
	AGENT_DELETE("agent.delete", AuditEventType.CLUSTER, "agent", false, BASIC),
	AGENT_UNBIND("agent.unbind", AuditEventType.CLUSTER, "agent", false, BASIC),
	AGENT_GROUP_UPDATE("agent_group.update", AuditEventType.CLUSTER, "agent_group", false, SUMMARY),
	AGENT_GROUP_DELETE("agent_group.delete", AuditEventType.CLUSTER, "agent_group", false, BASIC),
	CLUSTER_NODE_REMOVE("cluster.node_remove", AuditEventType.CLUSTER, "service", false, BASIC),
	CLUSTER_MONITOR_UPDATE("cluster.monitor_update", AuditEventType.CLUSTER, "service", false, BASIC),

	// 审计能力自身
	AUDIT_EXPORT("audit.export", AuditEventType.AUDIT, "audit_log", false, BASIC),
	AUDIT_ACCESS_DENIED("audit.access_denied", AuditEventType.AUDIT, "audit_log", false, BASIC);

	private final String key;
	private final AuditEventType eventType;
	private final String objectType;
	private final boolean async;
	private final AuditDetailLevel detailLevel;

	AuditAction(String key, AuditEventType eventType, String objectType, boolean async, AuditDetailLevel detailLevel) {
		this.key = key;
		this.eventType = eventType;
		this.objectType = objectType;
		this.async = async;
		this.detailLevel = detailLevel;
	}

	public String getKey() {
		return key;
	}

	public AuditEventType getEventType() {
		return eventType;
	}

	public String getObjectType() {
		return objectType;
	}

	/** 异步动作先记录受理事件，再由 SYSTEM 记录最终结果。 */
	public boolean isAsync() {
		return async;
	}

	public AuditDetailLevel getDetailLevel() {
		return detailLevel;
	}

	/** 页面文案键，例如 audit_action_task_start。 */
	public String messageKey() {
		return "audit_action_" + key.replace('.', '_');
	}

	public static AuditAction ofKey(String key) {
		for (AuditAction action : values()) {
			if (action.key.equals(key)) {
				return action;
			}
		}
		return null;
	}
}
