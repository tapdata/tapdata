package com.tapdata.tm.audit.query;

import com.tapdata.tm.audit.AuditBatch;
import com.tapdata.tm.audit.AuditBatchItem;
import com.tapdata.tm.audit.AuditOutcome;
import com.tapdata.tm.audit.AuditValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression coverage for the visibility boundary shared by audit list and detail. */
@Timeout(10)
class AuditVisibilityIT {

	private static final String USER = "requester";
	private static final String OTHER = "other-requester";
	private final AuditQueryService service = new AuditQueryService(null, null);

	@Test
	@DisplayName("本人 FAILED/SINGLE：仅保留本人记录，不暴露他人最终事件")
	void failed_single_must_not_expose_another_users_final() {
		AuditEventVo own = event("own-failed", USER, "SINGLE", "FAILED");
		AuditEventVo other = event("other-request", OTHER, "REQUEST", "ACCEPTED");
		AuditEventVo fin = event("other-final", AuditValues.SYSTEM, "FINAL", "SUCCESS");

		AuditOperationVo visible = visible(own, other, fin);
		assertEvents(visible, own);
		assertThat(visible.getOutcome()).isEqualTo("FAILED");
		assertThat(visible.isPending()).isFalse();
		assertThat(visible.getFinalTimestamp()).isNull();
	}

	@Test
	@DisplayName("非受理事件：不能授权读取 SYSTEM FINAL")
	void only_accepted_requests_can_authorize_system_finals() {
		for (String phase : new String[]{"SINGLE", "FINAL", "REQUEST"}) {
			for (String outcome : new String[]{"FAILED", "SUCCESS", null}) {
				AuditEventVo own = event("own", USER, phase, outcome);
				AuditOperationVo visible = visible(own, event("system", AuditValues.SYSTEM, "FINAL", "SUCCESS"));
				assertEvents(visible, own);
				assertThat(visible.getOutcome()).isEqualTo(outcome);
			}
		}
	}

	@Test
	@DisplayName("正常受理：保留关联 FINAL、本人发起人和最终结果")
	void accepted_request_retains_its_final_and_initiator() {
		for (String outcome : new String[]{"SUCCESS", "FAILED"}) {
			AuditEventVo request = event("request", USER, "REQUEST", "ACCEPTED");
			AuditEventVo fin = event("system", AuditValues.SYSTEM, "FINAL", outcome);
			fin.setTimestamp(new Date(2000));
			AuditOperationVo visible = visible(fin, request);
			assertEvents(visible, request, fin);
			assertThat(visible.getOutcome()).isEqualTo(outcome);
			assertThat(visible.getFinalTimestamp()).isEqualTo(fin.getTimestamp());
			assertThat(visible.isPending()).isFalse();
		}
	}

	@Test
	@DisplayName("跨用户同对象均受理：双方均不能读取 SYSTEM FINAL")
	void accepted_object_with_multiple_users_must_fail_closed_for_both() {
		AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
		AuditEventVo other = event("other", OTHER, "REQUEST", "ACCEPTED");
		other.setTimestamp(new Date(500));
		AuditEventVo fin = event("system", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		AuditOperationVo operation = operation(own, other, fin);

		for (AuditEventVo request : List.of(own, other)) {
			AuditOperationVo visible = service.visibleOperation(operation, request.getUserId()).orElseThrow();
			assertEvents(visible, request);
			assertThat(visible.getOutcome()).isEqualTo("ACCEPTED");
			assertThat(visible.isPending()).isTrue();
			assertThat(visible.getFinalTimestamp()).isNull();
		}
	}

	@Test
	@DisplayName("他人失败尝试：不影响本人已受理对象的唯一归属")
	void other_users_failed_attempt_does_not_steal_accepted_ownership() {
		AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
		AuditEventVo failed = event("failed", OTHER, "SINGLE", "FAILED");
		failed.setTimestamp(new Date(500));
		AuditEventVo fin = event("system", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		AuditOperationVo visible = visible(failed, own, fin);
		assertEvents(visible, own, fin);
		assertThat(visible.getOutcome()).isEqualTo("SUCCESS");
	}

	@Test
	@DisplayName("关联键不一致：operation、action、类型或对象任一不同均排除")
	void final_must_match_operation_action_type_and_object() {
		AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
		AuditEventVo operation = event("wrong-operation", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		operation.setOperationId("another-operation");
		AuditEventVo action = event("wrong-action", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		action.setAction("task.stop");
		AuditEventVo type = event("wrong-type", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		type.setObjectType("CLUSTER");
		AuditEventVo object = event("wrong-object", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		object.setObjectId("another-object");

		for (AuditEventVo fin : List.of(operation, action, type, object)) {
			AuditOperationVo visible = visible(own, fin);
			assertEvents(visible, own);
			assertThat(visible.getOutcome()).isEqualTo("ACCEPTED");
			assertThat(visible.isPending()).isTrue();
		}
	}

	@Test
	@DisplayName("SYSTEM 无对象或非 FINAL：保守排除")
	void system_event_must_be_a_final_with_an_explicit_object() {
		AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
		AuditEventVo noObject = event("no-object", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		noObject.setObjectId(null);
		AuditEventVo single = event("single", AuditValues.SYSTEM, "SINGLE", "SUCCESS");
		AuditEventVo request = event("request", AuditValues.SYSTEM, "REQUEST", "ACCEPTED");
		AuditOperationVo visible = visible(own, noObject, single, request);
		assertEvents(visible, own);
		assertThat(visible.getOutcome()).isEqualTo("ACCEPTED");
		assertThat(visible.isPending()).isTrue();
	}

	@Test
	@DisplayName("批量部分受理：失败项不能授权 FINAL，整体仍为部分成功")
	void batch_failed_items_must_not_authorize_finals() {
		AuditEventVo own = event("batch", USER, "REQUEST", "PARTIAL");
		own.setObjectId("accepted,failed");
		own.setBatch(batch("accepted", AuditOutcome.ACCEPTED, "failed", AuditOutcome.FAILED));
		AuditEventVo accepted = event("accepted-final", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		accepted.setObjectId("accepted");
		AuditEventVo failed = event("failed-final", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		failed.setObjectId("failed");
		AuditOperationVo visible = visible(own, accepted, failed);
		assertEvents(visible, own, accepted);
		assertThat(visible.getOutcome()).isEqualTo("PARTIAL");
		assertThat(visible.isPending()).isFalse();
	}

	@Test
	@DisplayName("批量归属冲突：逐对象检查，仅放行唯一归属的 FINAL")
	void batch_ownership_conflicts_are_checked_per_object() {
		AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
		own.setBatch(batch("shared", AuditOutcome.ACCEPTED, "exclusive", AuditOutcome.ACCEPTED));
		AuditEventVo other = event("other", OTHER, "REQUEST", "PARTIAL");
		other.setBatch(batch("shared", AuditOutcome.ACCEPTED, "exclusive", AuditOutcome.FAILED));
		AuditEventVo shared = event("shared-final", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		shared.setObjectId("shared");
		AuditEventVo exclusive = event("exclusive-final", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		exclusive.setObjectId("exclusive");
		AuditOperationVo visible = visible(own, other, shared, exclusive);
		assertEvents(visible, own, exclusive);
		assertThat(visible.getOutcome()).isEqualTo("ACCEPTED");
		assertThat(visible.isPending()).isTrue();
	}

	@Test
	@DisplayName("多对象 FINAL：每个对象均须唯一归属本人")
	void final_with_multiple_objects_requires_ownership_of_every_object() {
		AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
		AuditEventVo fin = event("system", AuditValues.SYSTEM, "FINAL", "SUCCESS");
		fin.setBatch(batch("object-1", AuditOutcome.SUCCESS, "foreign", AuditOutcome.SUCCESS));
		assertEvents(visible(own, fin), own);
	}

	@Test
	@DisplayName("关联字段或批量结果缺失：不能授权 FINAL")
	void missing_identity_or_batch_outcome_must_not_authorize_finals() {
		for (String missing : List.of("operation", "action", "type", "object", "batch-outcome")) {
			AuditEventVo own = event("own", USER, "REQUEST", "ACCEPTED");
			AuditEventVo fin = event("system", AuditValues.SYSTEM, "FINAL", "SUCCESS");
			switch (missing) {
				case "operation" -> { own.setOperationId(null); fin.setOperationId(null); }
				case "action" -> { own.setAction(null); fin.setAction(null); }
				case "type" -> { own.setObjectType(null); fin.setObjectType(null); }
				case "object" -> { own.setObjectId(" "); fin.setObjectId(" "); }
				case "batch-outcome" -> own.setBatch(batch("object-1", null, "failed", AuditOutcome.FAILED));
			}
			assertEvents(visible(own, fin), own);
		}
	}

	@Test
	@DisplayName("无本人事件：不返回他人操作")
	void foreign_events_without_an_own_request_are_invisible() {
		AuditOperationVo operation = operation(event("other", OTHER, "REQUEST", "ACCEPTED"),
				event("system", AuditValues.SYSTEM, "FINAL", "SUCCESS"));
		assertThat(service.visibleOperation(operation, USER)).isEmpty();
	}

	private AuditOperationVo visible(AuditEventVo... events) {
		return service.visibleOperation(operation(events), USER).orElseThrow();
	}

	private static AuditOperationVo operation(AuditEventVo... events) {
		return AuditQueryService.aggregate("legacy-operation|task.start", new ArrayList<>(List.of(events)));
	}

	private static void assertEvents(AuditOperationVo visible, AuditEventVo... expected) {
		assertThat(visible.getEvents()).containsExactly(expected);
		assertThat(visible.getInitiator()).isSameAs(expected[0]);
		assertThat(visible.getInitiator().getUserId()).isNotEqualTo(AuditValues.SYSTEM);
	}

	private static AuditBatch batch(String firstId, AuditOutcome firstOutcome, String secondId, AuditOutcome secondOutcome) {
		AuditBatch batch = new AuditBatch();
		batch.add(new AuditBatchItem(firstId, firstId, firstOutcome, null));
		batch.add(new AuditBatchItem(secondId, secondId, secondOutcome, null));
		return batch;
	}

	private static AuditEventVo event(String id, String user, String phase, String outcome) {
		AuditEventVo event = new AuditEventVo();
		event.setEventId(id);
		event.setUserId(user);
		event.setPhase(phase);
		event.setOutcome(outcome);
		event.setOperationId("legacy-operation");
		event.setAction("task.start");
		event.setObjectType("TASK");
		event.setObjectId("object-1");
		event.setTimestamp(new Date("REQUEST".equals(phase) ? 1000 : 2000));
		return event;
	}
}
