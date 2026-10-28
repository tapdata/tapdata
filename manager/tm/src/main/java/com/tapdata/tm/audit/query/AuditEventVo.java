package com.tapdata.tm.audit.query;

import com.tapdata.tm.audit.AuditBatch;
import com.tapdata.tm.audit.AuditChange;
import com.tapdata.tm.audit.AuditNode;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

/** 一条原始事件的展示视图；legacy=true 表示升级前的历史记录，缺失字段为 null，由页面显示历史说明。 */
@Data
public class AuditEventVo {
	private boolean legacy;
	private String eventId;
	private String operationId;
	private String phase;
	private Date timestamp;
	private String eventType;
	private String action;
	private String actionName;
	private String outcome;
	private String source;
	private String userId;
	private String username;
	private String authType;
	private String attemptedAccount;
	private String credentialRef;
	private String sourceIp;
	private String sourceIpNote;
	private String forwardedFor;
	private String userAgent;
	private String objectType;
	private String objectId;
	private String objectName;
	private String reason;
	private String detailLevel;
	private String summary;
	private List<AuditChange> changes;
	private AuditBatch batch;
	private AuditNode node;
	private Map<String, String> context;
	/** 历史记录的原自然语言描述。 */
	private String legacyMessage;
}
