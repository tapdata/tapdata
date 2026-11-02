package com.tapdata.tm.audit;

import com.tapdata.tm.userLog.entity.UserLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** L2：批量操作作为一个逻辑操作，记录总数、成功、失败与对象级原因（需求 US-05、18.3-5/6）。 */
@Timeout(10)
class AuditBatchOperationIT {
	private final AuditItFixture fixture = new AuditItFixture();

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	@Test
	@DisplayName("批量删除中每个对象在业务内同步完成时，REQUEST 与 FINAL 可关联且不残留登记")
	void should_link_batch_delete_request_and_final() {
		try (GenericApplicationContext context = AuditWebContext.aspectContext(fixture)) {
			AuditDemoController controller = context.getBean(AuditDemoController.class);
			String a = AuditItFixture.oid(), b = AuditItFixture.oid();
			// 模拟 afterRemove：每个对象在业务方法内部完成
			controller.setInstantDelete(id -> fixture.finalizer.finishOrRecord("task", id, "orders",
					AuditAction.TASK_DELETE, AuditItFixture.user("u1", "ops@corp.com", "username_login")));
			AuditWebContext.begin("", "10.6.0.2", null, AuditItFixture.user("u1", "ops@corp.com", "username_login"));
			controller.batchDeleteCompletedInline(List.of(a, b));

			// 两个对象各有一条 FINAL，且与 REQUEST 同 Operation
			var finals = fixture.events(AuditAction.TASK_DELETE).stream()
					.filter(e -> "FINAL".equals(e.getPhase())).toList();
			assertThat(finals).hasSize(2);
			var request = fixture.events(AuditAction.TASK_DELETE).stream()
					.filter(e -> "REQUEST".equals(e.getPhase())).findFirst().orElseThrow();
			assertThat(finals).allSatisfy(f -> assertThat(f.getOperationId()).isEqualTo(request.getOperationId()));
			// 完成后不残留登记
			assertThat(fixture.collection(AuditOperationRegistry.COLLECTION)).isEmpty();
		}
	}

	@Test
	@DisplayName("3 个任务中 1 个失败时记为部分成功，含 2 成功 1 失败与失败原因；全部失败为失败；成功对象登记最终结果")
	void should_record_partial_success_with_item_results() {
		try (GenericApplicationContext context = AuditWebContext.aspectContext(fixture)) {
			AuditDemoController controller = context.getBean(AuditDemoController.class);
			String a = AuditItFixture.oid(), b = AuditItFixture.oid(), c = AuditItFixture.oid();
			AuditWebContext.begin("", "10.6.0.1", null, AuditItFixture.user("u1", "ops@corp.com", "username_login"));
			controller.batchStart(List.of(a, b, c), List.of(b));
			// 失败项不留下待完成登记（TAP-13018 review）
			assertThat(fixture.registry.peek("task", a)).isNotNull();
			assertThat(fixture.registry.peek("task", b)).isNull();
			controller.batchDelete(List.of(a, b));

			UserLogs start = fixture.only(AuditAction.TASK_START);
			assertThat(start.getOutcome()).isEqualTo("PARTIAL");
			assertThat(start.getBatch().getTotal()).isEqualTo(3);
			assertThat(start.getBatch().getSuccess()).isEqualTo(2);
			assertThat(start.getBatch().getFailed()).isEqualTo(1);
			AuditBatchItem failed = start.getBatch().getItems().stream().filter(i -> i.getOutcome() == AuditOutcome.FAILED).findFirst().orElseThrow();
			assertThat(failed.getObjectId()).isEqualTo(b);
			assertThat(failed.getReason()).isEqualTo("task is running");

			// 只操作一个对象时补齐顶层对象字段（F-02：前端单行操作也走批量接口）
			String single = AuditItFixture.oid();
			fixture.put("Task", new org.bson.Document("_id", new org.bson.types.ObjectId(single)).append("name", "orders-sync"));
			controller.batchStart(java.util.List.of(single), java.util.List.of());
			UserLogs one = fixture.events(AuditAction.TASK_START).stream()
					.filter(e -> single.equals(e.getObjectId())).findFirst().orElseThrow();
			assertThat(one.getObjectId()).isEqualTo(single);
			assertThat(one.getObjectName()).isEqualTo("orders-sync");
			assertThat(one.getBatch().getTotal()).isEqualTo(1);

			UserLogs delete = fixture.only(AuditAction.TASK_DELETE);
			assertThat(delete.getOutcome()).isEqualTo("FAILED");
			assertThat(delete.getBatch().getFailed()).isEqualTo(2);
		}
	}
}
