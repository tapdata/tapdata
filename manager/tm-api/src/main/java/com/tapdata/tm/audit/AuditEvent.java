package com.tapdata.tm.audit;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 一条原始审计事件。字段口径见需求 10.1；actor/source/sourceIp 未设置时由服务按请求上下文补齐。 */
@Data
public class AuditEvent {
	private String eventId;
	private String operationId;
	private AuditPhase phase = AuditPhase.SINGLE;
	private AuditAction action;
	private AuditOutcome outcome;
	private AuditSource source;
	private String userId;
	private String username;
	private String authType;
	/** 失败登录时脱敏后的尝试账号。 */
	private String attemptedAccount;
	/** API 调用身份或凭据的安全化标识。 */
	private String credentialRef;
	private String sourceIp;
	private String sourceIpNote;
	private String forwardedFor;
	private String userAgent;
	private Date timestamp;
	private String objectType;
	private String objectId;
	private String objectName;
	/** 安全化的失败或拒绝原因（i18n 键或已脱敏文本）。 */
	private String reason;
	private AuditDetailLevel detailLevel;
	private String summary;
	private List<AuditChange> changes = new ArrayList<>();
	private AuditBatch batch;
	private AuditNode node;
	private Map<String, String> context = new LinkedHashMap<>();

	public static AuditEvent of(AuditAction action, AuditOutcome outcome) {
		AuditEvent event = new AuditEvent();
		event.setAction(action);
		event.setOutcome(outcome);
		event.setObjectType(action.getObjectType());
		event.setDetailLevel(action.getDetailLevel());
		return event;
	}

	public AuditEvent object(String id, String name) {
		this.objectId = id;
		this.objectName = name;
		return this;
	}

	public AuditEvent system() {
		this.source = AuditSource.SYSTEM;
		this.userId = AuditValues.SYSTEM;
		this.username = AuditValues.SYSTEM;
		return this;
	}
}
