package com.tapdata.tm.audit;

import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Uses an explicitly configured isolated MongoDB, never application data. */
@Timeout(30)
@EnabledIfSystemProperty(named = "audit.it.mongoUri", matches = ".+")
class AuditRegistryMongoIT {
	@Test
	void real_unique_key_and_owned_consumption_preserve_overlapping_operations() throws Exception {
		try (var client = MongoClients.create(System.getProperty("audit.it.mongoUri"))) {
			var template = new MongoTemplate(client, "audit_review_" + UUID.randomUUID().toString().replace("-", ""));
			var executor = Executors.newFixedThreadPool(2);
			try {
				var first = new AuditOperationRegistry(template);
				var second = new AuditOperationRegistry(template);
				// Create the collection before the competing inserts, as production initialization does.
				template.createCollection(AuditOperationRegistry.COLLECTION);
				for (int i = 0; i < 20; i++) {
					String object = "task-" + i;
					var barrier = new CyclicBarrier(2);
					var a = executor.submit(() -> {
						barrier.await();
						return first.registerIfAbsent("task", object, "a", AuditAction.TASK_START, "orders");
					});
					var b = executor.submit(() -> {
						barrier.await();
						return second.registerIfAbsent("task", object, "b", AuditAction.TASK_STOP, "orders");
					});
					assertThat((a.get() ? 1 : 0) + (b.get() ? 1 : 0)).isEqualTo(1);
					String owner = first.peek("task", object).getOperationId();
					String another = owner.equals("a") ? "b" : "a";
					second.register("task", object, another, AuditAction.TASK_STOP, "orders");
					assertThat(first.peek("task", object).getOperationId()).isEqualTo(owner);
					assertThat(first.takeOwned("task", object, "wrong-owner")).isNull();
					assertThat(second.takeOwned("task", object, owner).getOperationId()).isEqualTo(owner);
					assertThat(second.registerOperation("task", object, another, AuditAction.TASK_STOP, "orders")).isFalse();
					assertThat(first.pendingOperations("task", object)).hasSize(1);
					assertThat(first.takeOwned("task", object, another).getOperationId()).isEqualTo(another);
					assertThat(first.takeOwned("task", object, another)).isNull();
					assertThat(first.pendingOperations("task", object)).isEmpty();
				}
			} finally {
				executor.shutdownNow();
				template.getDb().drop();
			}
		}
	}
}
