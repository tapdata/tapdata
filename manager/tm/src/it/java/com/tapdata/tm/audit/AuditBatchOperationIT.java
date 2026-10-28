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
	@DisplayName("3 个任务中 1 个失败时记为部分成功，含 2 成功 1 失败与失败原因；全部失败为失败；成功对象登记最终结果")
	void should_record_partial_success_with_item_results() {
		try (GenericApplicationContext context = AuditWebContext.aspectContext(fixture)) {
			AuditDemoController controller = context.getBean(AuditDemoController.class);
			String a = AuditItFixture.oid(), b = AuditItFixture.oid(), c = AuditItFixture.oid();
			AuditWebContext.begin("", "10.6.0.1", null, AuditItFixture.user("u1", "ops@corp.com", "username_login"));
			controller.batchStart(List.of(a, b, c), List.of(b));
			controller.batchDelete(List.of(a, b));

			UserLogs start = fixture.only(AuditAction.TASK_START);
			assertThat(start.getOutcome()).isEqualTo("PARTIAL");
			assertThat(start.getBatch().getTotal()).isEqualTo(3);
			assertThat(start.getBatch().getSuccess()).isEqualTo(2);
			assertThat(start.getBatch().getFailed()).isEqualTo(1);
			AuditBatchItem failed = start.getBatch().getItems().stream().filter(i -> i.getOutcome() == AuditOutcome.FAILED).findFirst().orElseThrow();
			assertThat(failed.getObjectId()).isEqualTo(b);
			assertThat(failed.getReason()).isEqualTo("task is running");
			assertThat(fixture.registry.peek("task", a)).isNotNull();
			assertThat(fixture.registry.peek("task", b)).isNull();

			UserLogs delete = fixture.only(AuditAction.TASK_DELETE);
			assertThat(delete.getOutcome()).isEqualTo("FAILED");
			assertThat(delete.getBatch().getFailed()).isEqualTo(2);
		}
	}
}
