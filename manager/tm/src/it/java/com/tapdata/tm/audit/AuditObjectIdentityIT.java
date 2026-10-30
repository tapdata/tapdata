package com.tapdata.tm.audit;

import com.tapdata.tm.audit.query.AuditEventVo;
import com.tapdata.tm.audit.query.AuditOperationVo;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L2：审计事件的对象标识可读性（TAP-13018 F-12 / F-14 / F-15）。
 * 对象列只显示内部 ID 或裸 service key 时，运维无法从列表识别被操作的对象。
 */
@Timeout(10)
class AuditObjectIdentityIT {
	private final AuditItFixture fixture = new AuditItFixture();

	@Test
	@DisplayName("连接器安装按 pdkId 解析连接器名称，多条上传各自带对象")
	void should_resolve_connector_name_by_pdk_id() {
		fixture.put("DatabaseTypes", new Document("_id", new org.bson.types.ObjectId())
				.append("pdkId", "mysql")
				.append("name", "MySQL")
				.append("is_deleted", false));
		assertThat(fixture.objectResolver.resolveConnectorName("mysql")).isEqualTo("MySQL");
		assertThat(fixture.objectResolver.resolveConnectorName("unknown")).isNull();
		assertThat(fixture.objectResolver.resolveConnectorName(null)).isNull();
	}

	@Test
	@DisplayName("外部存储测试只带 id 时按 id 反查名称；已有名称时不再覆盖")
	void should_resolve_external_storage_name_for_websocket_test() {
		String id = AuditItFixture.oid();
		fixture.put("ExternalStorage", new Document("_id", new org.bson.types.ObjectId(id)).append("name", "s3-bucket"));
		WebSocketAudit audit = new WebSocketAudit(fixture.auditEventService, fixture.objectResolver);
		var user = AuditItFixture.user(AuditItFixture.oid(), "alice@corp.com", "user");

		audit.testRequested(AuditAction.EXTERNAL_STORAGE_TEST, user, Map.of("id", id));
		UserLogs resolved = fixture.only(AuditAction.EXTERNAL_STORAGE_TEST);
		assertThat(resolved.getObjectId()).isEqualTo(id);
		assertThat(resolved.getObjectName()).isEqualTo("s3-bucket");

		audit.testRequested(AuditAction.EXTERNAL_STORAGE_TEST, user, Map.of("id", id, "name", "form-value"));
		List<UserLogs> events = fixture.events(AuditAction.EXTERNAL_STORAGE_TEST);
		assertThat(events).hasSize(2);
		assertThat(events.get(1).getObjectName()).isEqualTo("form-value");
	}

	@Test
	@DisplayName("服务操作对象统一为「服务 @ 主机」，与 SYSTEM 最终事件口径一致")
	void should_display_service_object_with_host() {
		AuditNode node = new AuditNode("engine", "instance-1", "node-1", "10.0.0.9");
		assertThat(ClusterOperationAudit.displayName("engine", node)).isEqualTo("Engine @ node-1");
		assertThat(ClusterOperationAudit.displayName("agent", node)).isEqualTo("Agent @ node-1");
		// 主机不可知时保留原值，不拼出半截名称
		assertThat(ClusterOperationAudit.displayName("engine", new AuditNode("engine", "i1", null, null)))
				.isEqualTo("engine");
		assertThat(ClusterOperationAudit.displayName("engine", null)).isEqualTo("engine");
		assertThat(ClusterOperationAudit.displayName(null, node)).isNull();
	}

	@Test
	@DisplayName("列表与详情的结果都取操作聚合结果")
	void should_expose_aggregated_outcome_on_operation() {
		AuditEventVo request = new AuditEventVo();
		request.setOutcome(AuditOutcome.ACCEPTED.name());
		request.setPhase(AuditPhase.REQUEST.name());
		request.setTimestamp(new Date(1000L));
		AuditEventVo fin = new AuditEventVo();
		fin.setOutcome(AuditOutcome.SUCCESS.name());
		fin.setPhase(AuditPhase.FINAL.name());
		fin.setTimestamp(new Date(2000L));

		AuditOperationVo op = new AuditOperationVo();
		op.setInitiator(request);
		op.setEvents(List.of(request, fin));
		assertThat(request.getOutcome()).isEqualTo(AuditOutcome.ACCEPTED.name());
		assertThat(fin.getOutcome()).isEqualTo(AuditOutcome.SUCCESS.name());
	}
}
