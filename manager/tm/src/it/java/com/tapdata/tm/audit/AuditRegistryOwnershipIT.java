package com.tapdata.tm.audit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(10)
class AuditRegistryOwnershipIT {
	private final AuditItFixture fixture = new AuditItFixture();

	@Test
	void register_must_preserve_first_owner() {
		fixture.registry.register("task", "shared", "first", AuditAction.TASK_START, "orders");
		fixture.registry.register("task", "shared", "second", AuditAction.TASK_STOP, "orders");
		assertThat(fixture.registry.peek("task", "shared").getOperationId()).isEqualTo("first");
		assertThat(fixture.registry.pendingOperations("task", "shared"))
				.extracting(AuditOperationRegistry.Pending::getOperationId).containsExactlyInAnyOrder("first", "second");
		fixture.registry.release("task", "shared", "second");
		assertThat(fixture.registry.peek("task", "shared").getOperationId()).isEqualTo("first");
	}

	@Test
	void retry_after_another_operation_finishes_must_not_duplicate_registration() {
		fixture.registry.register("task", "shared", "first", AuditAction.TASK_START, "orders");
		fixture.registry.register("task", "shared", "second", AuditAction.TASK_STOP, "orders");
		fixture.registry.takeOwned("task", "shared", "first");
		assertThat(fixture.registry.registerOperation("task", "shared", "second", AuditAction.TASK_STOP, "orders")).isFalse();
		assertThat(fixture.registry.pendingOperations("task", "shared")).hasSize(1);
		fixture.registry.release("task", "shared", "second");
		assertThat(fixture.registry.pendingOperations("task", "shared")).isEmpty();
	}

	@Test
	void two_instances_competing_for_primary_slot_have_one_winner() throws Exception {
		fixture.collection(AuditOperationRegistry.COLLECTION);
		var secondInstance = new AuditOperationRegistry(fixture.mongoTemplate);
		var barrier = new CyclicBarrier(2);
		var executor = Executors.newFixedThreadPool(2);
		try {
			var first = executor.submit(() -> {
				barrier.await();
				return fixture.registry.registerIfAbsent("task", "shared", "first", AuditAction.TASK_START, "orders");
			});
			var second = executor.submit(() -> {
				barrier.await();
				return secondInstance.registerIfAbsent("task", "shared", "second", AuditAction.TASK_STOP, "orders");
			});
			assertThat((first.get() ? 1 : 0) + (second.get() ? 1 : 0)).isEqualTo(1);
			assertThat(fixture.collection(AuditOperationRegistry.COLLECTION)).hasSize(1);
		} finally {
			executor.shutdownNow();
		}
	}
}
