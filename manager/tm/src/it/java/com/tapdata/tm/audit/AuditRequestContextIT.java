package com.tapdata.tm.audit;

import com.tapdata.tm.userLog.entity.UserLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/** L2：请求上下文过滤器与审计服务共同决定来源 IP、来源与必填字段（需求 10.1 / 10.4）。 */
@Timeout(10)
class AuditRequestContextIT {
	private final AuditItFixture fixture = new AuditItFixture();

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	private UserLogs record(String trusted, String remote, String forwarded) {
		AuditWebContext.begin(trusted, remote, forwarded, AuditItFixture.user(AuditItFixture.oid(), "u@corp.com", "username_login"));
		fixture.auditEventService.record(AuditEvent.of(AuditAction.ROLE_UPDATE, AuditOutcome.SUCCESS), AuditItFixture.user("u1", "u@corp.com", "username_login"));
		AuditWebContext.end();
		return fixture.events.get(fixture.events.size() - 1);
	}

	@Test
	@DisplayName("未配置可信代理时不信任转发头；对端为可信代理时取最右侧非代理地址；无法确认时为不可用")
	void should_resolve_source_ip_with_trusted_proxy_rules() {
		UserLogs direct = record("", "203.0.113.5", "1.2.3.4");
		assertThat(direct.getSourceIp()).isEqualTo("203.0.113.5");
		assertThat(direct.getForwardedFor()).isEqualTo("1.2.3.4");
		assertThat(direct.getSourceIpNote()).isNull();

		UserLogs proxied = record("10.0.0.0/8", "10.0.0.2", "9.9.9.9, 198.51.100.7, 10.0.0.3");
		assertThat(proxied.getSourceIp()).isEqualTo("198.51.100.7");
		assertThat(proxied.getSourceIpNote()).isEqualTo(SourceIpResolver.NOTE_TRUSTED_PROXY);

		UserLogs proxyNoHeader = record("10.0.0.2", "10.0.0.2", null);
		assertThat(proxyNoHeader.getSourceIp()).isEqualTo("10.0.0.2");
		assertThat(proxyNoHeader.getSourceIpNote()).isEqualTo(SourceIpResolver.NOTE_PROXY_WITHOUT_HEADER);

		UserLogs forged = record("10.0.0.2", "10.0.0.2", "not-an-ip");
		assertThat(forged.getSourceIp()).isEqualTo(AuditValues.UNAVAILABLE);
		assertThat(forged.getSourceIpNote()).isEqualTo(SourceIpResolver.NOTE_UNAVAILABLE);
	}

	@Test
	@DisplayName("UI 会话、访问码身份与系统事件分别标记为 UI、OpenAPI、SYSTEM，并补齐全部必填字段")
	void should_mark_ui_openapi_system_source() {
		AuditWebContext.begin("", "10.3.0.1", null, null);
		fixture.auditEventService.record(AuditEvent.of(AuditAction.TASK_UPDATE, AuditOutcome.SUCCESS), AuditItFixture.user("u1", "ui@corp.com", "username_login"));
		fixture.auditEventService.record(AuditEvent.of(AuditAction.TASK_UPDATE, AuditOutcome.SUCCESS), AuditItFixture.user("u2", "sdk@corp.com", "access_code"));
		AuditWebContext.end();
		AuditEvent system = AuditEvent.of(AuditAction.SERVICE_START, AuditOutcome.SUCCESS).system();
		system.setNode(new AuditNode("Engine", "p-1", "host-a", "172.16.0.9"));
		fixture.auditEventService.record(system, null);

		assertThat(fixture.events).extracting(UserLogs::getSource).containsExactly("UI", "OPENAPI", "SYSTEM");
		assertThat(fixture.events.get(2).getUserId()).isEqualTo(AuditValues.SYSTEM);
		assertThat(fixture.events.get(2).getSourceIp()).isEqualTo("172.16.0.9");
		assertThat(fixture.events).allSatisfy(e -> {
			assertThat(e.getEventId()).isNotBlank();
			assertThat(e.getOperationId()).isNotBlank();
			assertThat(e.getUserId()).isNotBlank();
			assertThat(e.getSource()).isNotBlank();
			assertThat(e.getSourceIp()).isNotBlank();
			assertThat(e.getTimestamp()).isNotNull();
			assertThat(e.getEventType()).isNotBlank();
			assertThat(e.getAction()).isNotBlank();
			assertThat(e.getObjectType()).isNotBlank();
			assertThat(e.getOutcome()).isNotBlank();
		});
	}
}
