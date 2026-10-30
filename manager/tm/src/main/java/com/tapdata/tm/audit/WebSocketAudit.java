package com.tapdata.tm.audit;

import com.tapdata.tm.config.security.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 经 WebSocket 发起的用户操作（数据源/外部存储测试连接）。WebSocket 消息没有 HTTP 请求上下文，
 * 来源固定为 UI，来源 IP 记为不可用；记录的是用户发起测试这一动作，测试结果由引擎异步回写连接状态。
 */
@Slf4j
@Component
public class WebSocketAudit {
	private final AuditEventService auditEventService;
	private AuditObjectResolver objectResolver;

	public WebSocketAudit(AuditEventService auditEventService) {
		this(auditEventService, null);
	}

	public WebSocketAudit(AuditEventService auditEventService, AuditObjectResolver objectResolver) {
		this.auditEventService = auditEventService;
		this.objectResolver = objectResolver;
	}

	@org.springframework.beans.factory.annotation.Autowired(required = false)
	public void setObjectResolver(AuditObjectResolver objectResolver) {
		this.objectResolver = objectResolver;
	}

	public void testRequested(AuditAction action, UserDetail user, Map<String, Object> data) {
		try {
			AuditEvent event = AuditEvent.of(action, AuditOutcome.SUCCESS);
			event.setSource(AuditSource.UI);
			event.setSourceIp(AuditValues.UNAVAILABLE);
			event.setSourceIpNote(SourceIpResolver.NOTE_UNAVAILABLE);
			Object id = data == null ? null : data.get("id");
			Object name = data == null ? null : data.get("name");
			String objectId = id == null ? null : String.valueOf(id);
			String objectName = name == null ? null : String.valueOf(name);
			if (objectName == null && objectId != null && action != null && objectResolver != null) {
				// 前端只传配置 id 时按对象类型反查名称，与 HTTP 侧口径一致（TAP-13018 F-15）
				objectName = objectResolver.resolveName(action.getObjectType(), objectId);
			}
			event.object(objectId, objectName);
			event.setSummary("audit_summary_test_dispatched");
			auditEventService.record(event, user);
		} catch (Exception e) {
			log.warn("Audit websocket test request failed", e);
		}
	}
}
