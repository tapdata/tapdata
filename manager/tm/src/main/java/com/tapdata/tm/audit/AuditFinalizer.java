package com.tapdata.tm.audit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * 异步操作的最终结果：只为已登记受理的用户请求写 SYSTEM 最终事件（同一 Operation ID），
 * 对象自身运行中的状态变化不在审计范围。最终状态以 TapData 能确认的结果为准，中间状态不记录。
 */
@Slf4j
@Component
public class AuditFinalizer implements AuditOperationFinisher {
	private static final Set<String> START_OK = Set.of("running", "complete");
	private static final Set<String> START_FAILED = Set.of("error", "schedule_failed");
	private static final Set<String> STOP_OK = Set.of("stop", "complete");
	private static final Set<String> RESET_OK = Set.of("wait_start", "edit");

	private final AuditEventService auditEventService;
	private final AuditOperationRegistry registry;

	public AuditFinalizer(AuditEventService auditEventService, AuditOperationRegistry registry) {
		this.auditEventService = auditEventService;
		this.registry = registry;
	}

	/** 任务状态机迁移后的回调；after 为新状态。 */
	public void onTaskStatus(String objectType, String taskId, String taskName, String after) {
		try {
			if (after == null) {
				return;
			}
			for (AuditOperationRegistry.Pending pending : registry.pendingOperations(objectType, taskId)) {
				AuditAction action = AuditAction.ofKey(pending.getAction());
				AuditOutcome outcome = taskOutcome(action, after);
				if (outcome != null) {
					AuditOperationRegistry.Pending consumed = registry.takeOwned(objectType, taskId, pending.getOperationId());
					if (consumed != null) {
						writeFinal(consumed, taskId, taskName, outcome,
								outcome == AuditOutcome.FAILED ? "audit_reason_status_" + after : null);
					}
				}
			}
		} catch (Exception e) {
			log.warn("Finalize task audit failed, task={}", taskId, e);
		}
	}

	static AuditOutcome taskOutcome(AuditAction action, String after) {
		if (action == null) {
			return null;
		}
		return switch (action) {
			case TASK_START -> START_OK.contains(after) ? AuditOutcome.SUCCESS
					: START_FAILED.contains(after) ? AuditOutcome.FAILED : null;
			case TASK_STOP, TASK_FORCE_STOP -> STOP_OK.contains(after) ? AuditOutcome.SUCCESS
					: "error".equals(after) ? AuditOutcome.FAILED : null;
			case TASK_RESET -> RESET_OK.contains(after) ? AuditOutcome.SUCCESS
					: "renew_failed".equals(after) ? AuditOutcome.FAILED : null;
			case TASK_DELETE, LIVE_CACHE_DELETE -> "delete_failed".equals(after) ? AuditOutcome.FAILED : null;
			default -> null;
		};
	}

	/**
	 * 异步删除完成。
	 *
	 * <p>有受理登记时消费该登记并写 SYSTEM 最终事件（与 REQUEST 同 Operation，可关联）。
	 * 没有登记说明这次删除不是由带审计的用户接口受理的（如定时任务），按发起用户记一条完成事件。
	 * 登记已存在即代表「用户请求已受理」，因此两条路径都不会在事后留下 pending（TAP-13018 review）。</p>
	 */
	public void finishOrRecord(String objectType, String objectId, String objectName, AuditAction fallback,
							   com.tapdata.tm.config.security.UserDetail user) {
		try {
			AuditOperationRegistry.Pending pending = registry.peek(objectType, objectId);
			if (pending != null) {
				finish(objectType, objectId, objectName, AuditOutcome.SUCCESS, null);
				return;
			}
			AuditEvent event = AuditEvent.of(fallback, AuditOutcome.SUCCESS);
			event.object(objectId, objectName);
			auditEventService.record(event, user);
		} catch (Exception e) {
			log.warn("Finalize audit failed, type={}, id={}", objectType, objectId, e);
		}
	}

	/** 写最终事件并移除登记；登记已被其它实例取走时不重复写。 */
	@Override
	public void finish(String objectType, String objectId, String objectName, AuditOutcome outcome, String reason) {
		for (AuditOperationRegistry.Pending pending : registry.pendingOperations(objectType, objectId)) {
			AuditOperationRegistry.Pending consumed = registry.takeOwned(objectType, objectId, pending.getOperationId());
			if (consumed != null) {
				writeFinal(consumed, objectId, objectName, outcome, reason);
			}
		}
	}

	private void writeFinal(AuditOperationRegistry.Pending pending, String objectId, String objectName,
							AuditOutcome outcome, String reason) {
		AuditAction action = AuditAction.ofKey(pending.getAction());
		if (action == null) {
			return;
		}
		AuditEvent event = AuditEvent.of(action, outcome).system();
		event.setPhase(AuditPhase.FINAL);
		event.setOperationId(pending.getOperationId());
		event.object(objectId, objectName != null ? objectName : pending.getObjectName());
		event.setReason(reason);
		auditEventService.record(event, null);
	}
}
