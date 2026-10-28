package com.tapdata.tm.audit;

import com.tapdata.tm.config.security.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 集群服务启停与升级：用户请求创建 ClusterOperation 时记录受理事件，Agent 回报或超时后由 SYSTEM 记录最终结果。
 * 登记键为 ClusterOperation ID，因此同一请求下发到多个节点时各节点分别给出最终结果。
 */
@Slf4j
@Component
public class ClusterOperationAudit {
	public static final String OBJECT_TYPE = "cluster_operation";
	/** Agent 升级的最终结果按 process_id 回报（done/fail），不带 ClusterOperation ID。 */
	public static final String UPGRADE_OBJECT_TYPE = "agent_upgrade";

	private final AuditEventService auditEventService;
	private final AuditOperationRegistry registry;
	private final AuditFinalizer finalizer;

	public ClusterOperationAudit(AuditEventService auditEventService, AuditOperationRegistry registry, AuditFinalizer finalizer) {
		this.auditEventService = auditEventService;
		this.registry = registry;
		this.finalizer = finalizer;
	}

	public static AuditAction actionOf(String operation) {
		if (operation == null) {
			return null;
		}
		return switch (operation) {
			case "start" -> AuditAction.SERVICE_START;
			case "stop" -> AuditAction.SERVICE_STOP;
			case "restart" -> AuditAction.SERVICE_RESTART;
			case "update", "upgrade" -> AuditAction.SERVICE_UPGRADE;
			default -> null;
		};
	}

	public void accepted(String operation, String clusterOperationId, String serviceName, AuditNode node, UserDetail user) {
		accepted(operation, OBJECT_TYPE, clusterOperationId, serviceName, node, user);
	}

	public void accepted(String operation, String objectType, String objectId, String serviceName, AuditNode node, UserDetail user) {
		try {
			AuditAction action = actionOf(operation);
			String clusterOperationId = objectId;
			if (action == null || clusterOperationId == null) {
				return;
			}
			String operationId = auditEventService.currentOperationId();
			AuditEvent event = AuditEvent.of(action, AuditOutcome.ACCEPTED);
			event.setPhase(AuditPhase.REQUEST);
			event.setOperationId(operationId);
			event.object(clusterOperationId, serviceName);
			event.setNode(node);
			auditEventService.record(event, user);
			registry.register(objectType, clusterOperationId, operationId, action, serviceName);
		} catch (Exception e) {
			log.warn("Audit cluster operation request failed", e);
		}
	}

	public void finished(String clusterOperationId, boolean success, String reason) {
		finished(OBJECT_TYPE, clusterOperationId, success, reason);
	}

	public void finished(String objectType, String objectId, boolean success, String reason) {
		try {
			String clusterOperationId = objectId;
			finalizer.finish(objectType, clusterOperationId, null, success ? AuditOutcome.SUCCESS : AuditOutcome.FAILED, success ? null : reason);
		} catch (Exception e) {
			log.warn("Audit cluster operation result failed", e);
		}
	}
}
