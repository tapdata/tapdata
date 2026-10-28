package com.tapdata.tm.audit;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** L2：系统设置保存经 SettingsChangeAudit 生成字段差异，秘密字段只记状态；无字段差异时如实标为基础审计（需求 12 章、US-06）。 */
@Timeout(10)
class AuditConfigChangeIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final SettingsChangeAudit audit = new SettingsChangeAudit(fixture.auditEventService, fixture.mongoTemplate,
			new AuditRetentionManager(fixture.mongoTemplate), new ModuleAccess(mock(PermissionService.class), mock(SettingsService.class)));

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	private static SettingsDto setting(String category, String key, Object value) {
		SettingsDto dto = new SettingsDto();
		dto.setId(AuditItFixture.oid());
		dto.setCategory(category);
		dto.setKey(key);
		dto.setValue(value);
		return dto;
	}

	@Test
	@DisplayName("修改普通设置记录前后值，修改 SMTP 密码只记已替换；未变化时记录基础审计与无字段变化说明；任务配置保存为基础审计")
	void should_record_settings_diff_and_basic_level_label() {
		fixture.put("Settings", new Document("_id", "s1").append("category", "SMTP").append("key", "smtp.server.host").append("value", "mail.a.com"));
		fixture.put("Settings", new Document("_id", "s2").append("category", "SMTP").append("key", "smtp.server.password").append("value", "old-secret"));
		fixture.put("Settings", new Document("_id", "s3").append("category", "Log").append("key", "log.upload.max").append("value", "100"));
		AuditWebContext.begin("", "10.7.0.1", null, AuditItFixture.user("u1", "admin@corp.com", "username_login"));
		boolean[] saved = {false};
		audit.save(List.of(setting("SMTP", "smtp.server.host", "mail.b.com"), setting("SMTP", "smtp.server.password", "new-secret"),
				setting("Log", "log.upload.max", "100")), AuditItFixture.user("u1", "admin@corp.com", "username_login"), () -> saved[0] = true);
		assertThat(saved[0]).isTrue();
		UserLogs event = fixture.only(AuditAction.SETTINGS_UPDATE);
		assertThat(event.getDetailLevel()).isEqualTo("DIFF");
		assertThat(event.getChanges()).hasSize(2);
		assertThat(event.getChanges()).anySatisfy(c -> {
			assertThat(c.getField()).isEqualTo("SMTP.smtp.server.host");
			assertThat(c.getBefore()).isEqualTo("mail.a.com");
			assertThat(c.getAfter()).isEqualTo("mail.b.com");
		});
		assertThat(event.getChanges()).anySatisfy(c -> {
			assertThat(c.getField()).isEqualTo("SMTP.smtp.server.password");
			assertThat(c.getSecretState()).isEqualTo(AuditValues.SECRET_REPLACED);
			assertThat(c.getBefore()).isNull();
			assertThat(c.getAfter()).isNull();
		});

		fixture.events.clear();
		audit.save(List.of(setting("Log", "log.upload.max", "100")), AuditItFixture.user("u1", "admin@corp.com", "username_login"), () -> {
		});
		UserLogs unchanged = fixture.only(AuditAction.SETTINGS_UPDATE);
		assertThat(unchanged.getDetailLevel()).isEqualTo("BASIC");
		assertThat(unchanged.getSummary()).isEqualTo("audit_summary_settings_no_field_changed");

		fixture.events.clear();
		fixture.auditEventService.record(AuditEvent.of(AuditAction.TASK_UPDATE, AuditOutcome.SUCCESS).object("t1", "orders"), AuditItFixture.user("u1", "a", "username_login"));
		assertThat(fixture.only(AuditAction.TASK_UPDATE).getDetailLevel()).isEqualTo("BASIC");
		assertThat(fixture.only(AuditAction.TASK_UPDATE).getChanges()).isNull();
	}
}
