package com.tapdata.tm.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** L2：秘密不进入审计记录——字段变更、失败原因、上下文与请求体（需求 12.4、US-07、18.2-5/6）。 */
@Timeout(10)
class AuditRedactionIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final ObjectMapper mapper = new ObjectMapper();

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	@Test
	@DisplayName("密码只记已替换；错误中的连接串、凭据参数、私钥与长令牌被移除；切面不保存请求体")
	void should_never_store_secrets_or_request_bodies() throws Exception {
		AuditWebContext.begin("", "10.8.0.1", null, AuditItFixture.user("u1", "dba@corp.com", "username_login"));
		AuditEvent event = AuditEvent.of(AuditAction.DATASOURCE_UPDATE, AuditOutcome.FAILED);
		event.setChanges(List.of(new AuditChange("config.password", "p@ss-old", "p@ss-new", null), new AuditChange("config.host", "db1", "db2", null)));
		event.setReason("connect failed: mongodb://root:S3cr3t@db1:27017/admin jdbc:mysql://db1:3306/app?user=root&password=S3cr3t "
				+ "token=abcdefgh -----BEGIN PRIVATE KEY-----MIIEvQIBADANBg-----END PRIVATE KEY----- "
				+ "eyJhbGciOiJIUzI1NiJ9eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ");
		event.getContext().put("accessCode", "3324cfdf-7d3e-4792-bd32-571638d4562f");
		fixture.auditEventService.record(event, AuditItFixture.user("u1", "dba@corp.com", "username_login"));

		try (GenericApplicationContext context = AuditWebContext.aspectContext(fixture)) {
			context.getBean(AuditDemoController.class).updateWithSecret(AuditItFixture.oid(), Map.of("name", "pg", "password", "Body-Secret-1"));
		}

		String stored = mapper.writeValueAsString(fixture.events);
		assertThat(stored).doesNotContain("S3cr3t", "p@ss-old", "p@ss-new", "abcdefgh", "MIIEvQIBADANBg", "eyJhbGciOiJIUzI1NiJ9",
				"3324cfdf-7d3e-4792-bd32-571638d4562f", "Body-Secret-1");
		UserLogs first = fixture.events.get(0);
		assertThat(first.getChanges()).anySatisfy(c -> assertThat(c.getSecretState()).isEqualTo(AuditValues.SECRET_REPLACED));
		assertThat(first.getChanges()).anySatisfy(c -> assertThat(c.getAfter()).isEqualTo("db2"));
		assertThat(first.getContext()).containsEntry("accessCode", AuditRedactor.MASK);
		assertThat(fixture.events.get(1).getObjectName()).isEqualTo("pg");
	}
}
