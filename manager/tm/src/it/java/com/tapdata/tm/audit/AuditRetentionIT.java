package com.tapdata.tm.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** L2：审计日志保留期——默认 90 天、30–3650 校验、collMod 生效、前后值事件与失败处理（需求 15 章、US-12）。 */
@Timeout(10)
class AuditRetentionIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final PermissionService permissions = mock(PermissionService.class);
	private final SettingsService settings = mock(SettingsService.class);
	private final AuditRetentionManager manager = new AuditRetentionManager(fixture.mongoTemplate);
	private final SettingsChangeAudit audit = new SettingsChangeAudit(fixture.auditEventService, fixture.mongoTemplate, manager,
			new ModuleAccess(permissions, settings));

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	private static SettingsDto retention(Object value) {
		SettingsDto dto = new SettingsDto();
		dto.setId("r1");
		dto.setCategory("Log");
		dto.setKey(AuditRetentionManager.SETTING_KEY);
		dto.setValue(value);
		return dto;
	}

	@Test
	@DisplayName("种子默认 90 天且限制 30–3650；有效修改先调整 TTL 再保存并记录前后值；越界与 TTL 调整失败均拒绝保存并记录失败；不产生清理事件")
	void should_apply_retention_range_and_record_before_after() throws Exception {
		JsonNode seed;
		try (InputStream in = new ClassPathResource("init/idaas/4.25-2.json").getInputStream()) {
			seed = new ObjectMapper().readTree(in);
		}
		JsonNode set = seed.get(0).get("updates").get(0).get("u");
		assertThat(set.get("$set").get("default_value").asInt()).isEqualTo(90);
		assertThat(set.get("$set").get("min").asInt()).isEqualTo(30);
		assertThat(set.get("$set").get("max").asInt()).isEqualTo(3650);
		assertThat(set.get("$setOnInsert").get("value").asInt()).isEqualTo(90);
		assertThat(manager.currentDays()).isEqualTo(90);

		fixture.put("Settings", new Document("_id", "r1").append("category", "Log").append("key", AuditRetentionManager.SETTING_KEY).append("value", 90));
		when(permissions.checkCurrentUserHasPermission(any(), any())).thenReturn(true);
		AuditWebContext.begin("", "10.10.0.1", null, AuditItFixture.user("u1", "admin@corp.com", "username_login"));
		boolean[] saved = {false};
		audit.save(List.of(retention(180)), AuditItFixture.user("u1", "admin@corp.com", "username_login"), () -> saved[0] = true);
		assertThat(saved[0]).isTrue();
		assertThat(fixture.commands).hasSize(1);
		Document collMod = fixture.commands.get(0);
		assertThat(collMod.getString("collMod")).isEqualTo("UserLogs");
		assertThat(((Document) collMod.get("index")).get("expireAfterSeconds")).isEqualTo(180L * 24 * 3600);
		UserLogs ok = fixture.only(AuditAction.AUDIT_RETENTION_UPDATE);
		assertThat(ok.getOutcome()).isEqualTo("SUCCESS");
		assertThat(ok.getChanges().get(0).getBefore()).isEqualTo("90");
		assertThat(ok.getChanges().get(0).getAfter()).isEqualTo("180");
		assertThat(fixture.events(AuditAction.SETTINGS_UPDATE)).isEmpty();

		for (Object invalid : new Object[]{10, 4000, "abc", 45.5}) {
			fixture.events.clear();
			saved[0] = false;
			assertThatThrownBy(() -> audit.save(List.of(retention(invalid)), AuditItFixture.user("u1", "a", "username_login"), () -> saved[0] = true))
					.isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("Audit.Retention.OutOfRange"));
			assertThat(saved[0]).isFalse();
			assertThat(fixture.only(AuditAction.AUDIT_RETENTION_UPDATE).getOutcome()).isEqualTo("FAILED");
		}

		fixture.events.clear();
		when(fixture.database.runCommand(any(Document.class))).thenThrow(new RuntimeException("not authorized on UserLogs to execute command"));
		assertThatThrownBy(() -> audit.save(List.of(retention(60)), AuditItFixture.user("u1", "a", "username_login"), () -> saved[0] = true))
				.isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("Audit.Retention.ApplyFailed"));
		UserLogs failed = fixture.only(AuditAction.AUDIT_RETENTION_UPDATE);
		assertThat(failed.getOutcome()).isEqualTo("FAILED");
		assertThat(failed.getReason()).isEqualTo("Audit.Retention.ApplyFailed");
		assertThat(fixture.events).hasSize(1);
	}
}
