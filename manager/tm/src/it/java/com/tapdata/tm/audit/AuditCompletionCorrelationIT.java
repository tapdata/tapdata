package com.tapdata.tm.audit;

import com.tapdata.tm.base.dto.ResponseMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(10)
class AuditCompletionCorrelationIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final ApiController controller;

	AuditCompletionCorrelationIT() {
		var proxy = new AspectJProxyFactory(new ApiController());
		proxy.addAspect(new AuditAspect(fixture.auditEventService, fixture.objectResolver, fixture.registry));
		controller = proxy.getProxy();
	}

	@AfterEach
	void clearContext() {
		AuditWebContext.end();
	}

	@Test
	void publishResultMustNotFinalizeConcurrentUnpublishOperation() {
		begin("publish-op");
		controller.publish("api-1");
		begin("unpublish-op");
		controller.unpublish("api-1");
		AuditWebContext.end();
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"publish-op", AuditOutcome.FAILED, "publish-configuration-error");
		assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
				.extracting(e -> e.getOperationId()).containsExactly("publish-op");
		assertThat(fixture.registry.pendingOperations("api", "api-1"))
				.extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("unpublish-op");
	}

	@Test
	void superseded_publish_stays_failed_when_its_success_report_arrives_late() {
		begin("publish-v1");
		controller.publish("api-1");
		begin("publish-v2");
		controller.publish("api-1");
		AuditWebContext.end();
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.FAILED, "Superseded; final state was not confirmed");
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.SUCCESS, "");
		assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
				.singleElement().satisfies(event -> {
					assertThat(event.getOperationId()).isEqualTo("publish-v1");
					assertThat(event.getOutcome()).isEqualTo("FAILED");
				});
		assertThat(fixture.registry.pendingOperations("api", "api-1"))
				.extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("publish-v2");
	}

	@Test
	void old_report_must_not_finish_a_later_publish_even_if_it_is_the_only_pending_operation() {
		begin("publish-v1");
		controller.publish("api-1");
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.SUCCESS, null);
		begin("publish-v2");
		controller.publish("api-1");
		AuditWebContext.end();
		fixture.finalizer.finish("api", "api-1", null, AuditOutcome.FAILED, "late-v1-report");
		assertThat(fixture.registry.pendingOperations("api", "api-1"))
				.extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("publish-v2");
		assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase())).hasSize(1);
	}

	@Test
	void exact_operation_and_action_finish_only_that_publish_version() {
		begin("publish-v1");
		controller.publish("api-1");
		begin("publish-v2");
		controller.publish("api-1");
		begin("unpublish-op");
		controller.unpublish("api-1");
		AuditWebContext.end();
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.FAILED, "v1-only");
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.FAILED, "duplicate");
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_PUBLISH,
				"unpublish-op", AuditOutcome.FAILED, "wrong-action");
		assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
				.extracting(e -> e.getOperationId()).containsExactly("publish-v1");
		assertThat(fixture.registry.pendingOperations("api", "api-1"))
				.extracting(AuditOperationRegistry.Pending::getOperationId)
				.containsExactlyInAnyOrder("publish-v2", "unpublish-op");
		fixture.finalizer.finishOperation("api", "api-1", null, AuditAction.API_UNPUBLISH,
				"unpublish-op", AuditOutcome.SUCCESS, null);
		assertThat(fixture.registry.pendingOperations("api", "api-1"))
				.extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("publish-v2");
	}

	@Test
	void engine_delete_report_must_not_use_the_reporting_http_requests_operation() {
		fixture.registry.register("task", "task-1", "delete-op", AuditAction.TASK_DELETE, "orders");
		fixture.registry.register("task", "task-1", "start-op", AuditAction.TASK_START, "orders");
		begin("engine-http-report");
		fixture.finalizer.finishOrRecord("task", "task-1", "orders", AuditAction.TASK_DELETE,
				AuditItFixture.user("u1", "ops@example.com", "username_login"));
		assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
				.extracting(e -> e.getOperationId()).containsExactly("delete-op");
		assertThat(fixture.registry.pendingOperations("task", "task-1"))
				.extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("start-op");
	}

	@Test
	void object_only_report_with_ambiguous_actions_must_not_broadcast() {
		fixture.registry.register("inspect", "i1", "start", AuditAction.INSPECT_START, "inspect");
		fixture.registry.register("inspect", "i1", "stop", AuditAction.INSPECT_STOP, "inspect");
		fixture.finalizer.finish("inspect", "i1", null, AuditOutcome.FAILED, "unknown-owner");
		assertThat(fixture.events).isEmpty();
		assertThat(fixture.registry.pendingOperations("inspect", "i1")).hasSize(2);
	}

	@Test
	void uncorrelated_report_must_not_finish_overlapping_publish_versions() {
		begin("publish-v1");
		controller.publish("api-1");
		begin("publish-v2");
		controller.publish("api-1");
		AuditWebContext.end();
		fixture.finalizer.finish("api", "api-1", null, AuditOutcome.SUCCESS, null);
		assertThat(fixture.events).noneMatch(e -> "FINAL".equals(e.getPhase()));
		assertThat(fixture.registry.pendingOperations("api", "api-1")).hasSize(2);
	}

	private void begin(String operationId) {
		AuditWebContext.end();
		AuditWebContext.begin("", "10.0.0.1", null, AuditItFixture.user("u1", "ops@example.com", "username_login"));
		AuditRequestContext.current().setOperationId(operationId);
	}

	public static class ApiController {
		@Audited(value = AuditAction.API_PUBLISH, objectId = "#id")
		public ResponseMessage<Void> publish(String id) {
			return new ResponseMessage<>();
		}

		@Audited(value = AuditAction.API_UNPUBLISH, objectId = "#id")
		public ResponseMessage<Void> unpublish(String id) {
			return new ResponseMessage<>();
		}
	}
}
