package com.tapdata.tm.audit;

import com.tapdata.tm.config.security.UserDetail;

/**
 * 统一审计写入入口。写入失败不得改变业务结果，但必须可观察（错误日志与限频系统告警），不能静默表现为成功。
 */
public interface AuditEventService {

	/** 记录事件；userDetail 为 null 时按 SYSTEM 或未认证规则补齐操作人。返回 eventId。 */
	String record(AuditEvent event, UserDetail userDetail);

	/** 为异步操作生成或复用当前请求的 Operation ID。 */
	String currentOperationId();
}
