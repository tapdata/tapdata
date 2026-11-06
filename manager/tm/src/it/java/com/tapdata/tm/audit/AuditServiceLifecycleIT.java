package com.tapdata.tm.audit;

import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/** L2：服务生命周期 SYSTEM 事件——心跳、组件停止上报与失联检测经 AuditServiceStates 状态跃迁记录（需求 9.9、US-09）。 */
@Timeout(10)
class AuditServiceLifecycleIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final ServiceLifecycleAudit lifecycle = new ServiceLifecycleAudit(fixture.auditEventService, fixture.mongoTemplate);

	@Test
	@DisplayName("另一 TM 判定失联后，本实例的恢复心跳仍要写恢复事件（TAP-13018 review P2）")
	void should_emit_reconnect_when_another_instance_marked_lost() {
		// 两个 TM 实例共享同一 Mongo 状态集合
		ServiceLifecycleAudit tmB = new ServiceLifecycleAudit(fixture.auditEventService, fixture.mongoTemplate);
		lifecycle.onHeartbeat("connector", "proc-9", "engine-host", "172.16.1.9");
		tmB.onHeartbeat("connector", "proc-9", "engine-host", "172.16.1.9");
		assertThat(fixture.events(AuditAction.SERVICE_START)).hasSize(1);

		// B 判定该 worker 失联，共享状态改为 LOST，但 A 的本地 knownUp 仍为 UP
		fixture.put("Workers", new Document("_id", "w9").append("process_id", "proc-9")
				.append("worker_type", "connector").append("ping_time", 1L));
		tmB.detectLost();
		assertThat(fixture.events(AuditAction.SERVICE_DISCONNECT)).hasSize(1);

		// 恢复心跳到达 A：不能被本地缓存吞掉
		lifecycle.onHeartbeat("connector", "proc-9", "engine-host", "172.16.1.9");
		assertThat(fixture.events(AuditAction.SERVICE_RECONNECT)).hasSize(1);
	}

	@Test
	@DisplayName("Engine 首次心跳为启动、重复心跳不记录、正常停止为停止、心跳超时为失联、再次心跳为恢复连接；TM 自身启停为 SYSTEM 事件")
	void should_record_system_start_stop_and_lost_events() {
		lifecycle.onHeartbeat("connector", "proc-1", "engine-host", "172.16.1.5");
		lifecycle.onHeartbeat("connector", "proc-1", "engine-host", "172.16.1.5");
		lifecycle.onStopped("connector", "proc-1", null, null);
		lifecycle.onStopped("connector", "proc-1", null, null);
		lifecycle.onHeartbeat("connector", "proc-1", "engine-host", "172.16.1.5");

		fixture.put("Workers", new Document("_id", "w1").append("process_id", "proc-1").append("worker_type", "connector").append("ping_time", 1L));
		lifecycle.detectLost();
		lifecycle.detectLost();
		lifecycle.onHeartbeat("connector", "proc-1", "engine-host", "172.16.1.5");
		lifecycle.onHeartbeat("api-server", "api-1", "api-host", "172.16.1.6");
		lifecycle.onManagementStarted();
		lifecycle.onManagementStopped();

		assertThat(fixture.events).extracting(UserLogs::getAction).containsExactly(
				"service.start", "service.stop", "service.start", "service.disconnect", "service.reconnect", "service.start",
				"service.start", "service.stop");
		assertThat(fixture.events).allSatisfy(e -> {
			assertThat(e.getUserId()).isEqualTo(AuditValues.SYSTEM);
			assertThat(e.getSource()).isEqualTo("SYSTEM");
			assertThat(e.getNode()).isNotNull();
		});
		UserLogs lost = fixture.only(AuditAction.SERVICE_DISCONNECT);
		assertThat(lost.getOutcome()).isEqualTo("FAILED");
		assertThat(lost.getReason()).isEqualTo("audit_reason_heartbeat_timeout");
		assertThat(lost.getNode().getService()).isEqualTo("Engine");
		assertThat(lost.getSourceIp()).isEqualTo("172.16.1.5");
		assertThat(fixture.events.get(5).getNode().getService()).isEqualTo("API Server");
		assertThat(fixture.events.get(6).getNode().getService()).isEqualTo("Management Service");
	}
}
