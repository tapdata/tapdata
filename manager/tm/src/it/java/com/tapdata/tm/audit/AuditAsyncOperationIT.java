package com.tapdata.tm.audit;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** L2：异步两阶段——切面受理登记、状态机/集群回报经 AuditFinalizer 写 SYSTEM 最终事件（需求 US-04、18.3-2/4）。 */
@Timeout(10)
class AuditAsyncOperationIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private GenericApplicationContext context;
	private AuditDemoController controller;

	@BeforeEach
	void setUp() {
		context = AuditWebContext.aspectContext(fixture);
		controller = context.getBean(AuditDemoController.class);
	}

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
		context.close();
	}

	@Test
	@DisplayName("启动任务先记已受理，任务进入运行或启动失败后由 SYSTEM 记录最终结果，两条事件共用 Operation ID 且只写一次")
	void should_link_request_and_final_result_by_operation_id() {
		String ok = AuditItFixture.oid();
		String bad = AuditItFixture.oid();
		AuditWebContext.begin("", "10.5.0.1", null, AuditItFixture.user("u1", "ops@corp.com", "username_login"));
		controller.start(ok);
		AuditWebContext.end();
		AuditWebContext.begin("", "10.5.0.1", null, AuditItFixture.user("u1", "ops@corp.com", "username_login"));
		controller.start(bad);
		AuditWebContext.end();

		fixture.finalizer.onTaskStatus("task", ok, "orders", "scheduling");
		fixture.finalizer.onTaskStatus("task", ok, "orders", "running");
		fixture.finalizer.onTaskStatus("task", ok, "orders", "running");
		fixture.finalizer.onTaskStatus("task", bad, "users", "schedule_failed");

		var events = fixture.events(AuditAction.TASK_START);
		assertThat(events).hasSize(4);
		UserLogs request = events.get(0);
		UserLogs finalOk = events.stream().filter(e -> "FINAL".equals(e.getPhase()) && ok.equals(e.getObjectId())).findFirst().orElseThrow();
		assertThat(request.getPhase()).isEqualTo("REQUEST");
		assertThat(request.getOutcome()).isEqualTo("ACCEPTED");
		assertThat(request.getUserId()).isEqualTo("u1");
		assertThat(finalOk.getOperationId()).isEqualTo(request.getOperationId());
		assertThat(finalOk.getUserId()).isEqualTo(AuditValues.SYSTEM);
		assertThat(finalOk.getOutcome()).isEqualTo("SUCCESS");
		UserLogs finalBad = events.stream().filter(e -> "FINAL".equals(e.getPhase()) && bad.equals(e.getObjectId())).findFirst().orElseThrow();
		assertThat(finalBad.getOutcome()).isEqualTo("FAILED");
		assertThat(finalBad.getReason()).isEqualTo("audit_reason_status_schedule_failed");
		assertThat(fixture.collection(AuditOperationRegistry.COLLECTION)).isEmpty();

		String operation = AuditItFixture.oid();
		ClusterOperationAudit cluster = new ClusterOperationAudit(fixture.auditEventService, fixture.registry, fixture.finalizer);
		AuditWebContext.begin("", "10.5.0.2", null, AuditItFixture.user("u2", "admin@corp.com", "username_login"));
		cluster.accepted("stop", operation, "engine", new AuditNode("engine", "uuid-1", "host-a", "172.16.0.3"), AuditItFixture.user("u2", "admin@corp.com", "username_login"));
		AuditWebContext.end();
		cluster.finished(operation, false, "audit_reason_timeout");
		var stop = fixture.events(AuditAction.SERVICE_STOP);
		assertThat(stop).extracting(UserLogs::getOutcome).containsExactly("ACCEPTED", "FAILED");
		assertThat(stop.get(1).getOperationId()).isEqualTo(stop.get(0).getOperationId());
	}

	@Test
	@DisplayName("请求在权限或状态校验阶段失败时只有一条失败事件，不登记最终结果")
	void should_write_single_failure_when_validation_fails() {
		String id = AuditItFixture.oid();
		AuditWebContext.begin("", "10.5.0.3", null, AuditItFixture.user("u1", "ops@corp.com", "username_login"));
		assertThatThrownBy(() -> controller.startRejected(id)).isInstanceOf(BizException.class);
		AuditWebContext.end();
		fixture.finalizer.onTaskStatus("task", id, "x", "running");

		UserLogs only = fixture.only(AuditAction.TASK_START);
		assertThat(only.getOutcome()).isEqualTo("FAILED");
		assertThat(only.getPhase()).isEqualTo("SINGLE");
		assertThat(only.getReason()).isEqualTo("NotAuthorized");
		assertThat(fixture.collection(AuditOperationRegistry.COLLECTION)).isEmpty();
	}
}
