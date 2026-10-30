package com.tapdata.tm.audit;

import com.tapdata.tm.userLog.entity.UserLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

/** L2：登录与 API 认证事件经真实 LoginAuditRecorder、AccessFailureAuditor、AuditContextFilter 与审计服务落库（需求 US-02）。 */
@Timeout(10)
class AuditAuthenticationIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final LoginAuditRecorder recorder = new LoginAuditRecorder(fixture.auditEventService);

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	@Test
	@DisplayName("登录成功记录真实用户、来源 IP 与登录方式；各类失败记录未认证、脱敏尝试账号与安全化原因")
	void should_record_login_success_and_each_failure_reason() {
		AuditWebContext.begin("", "10.1.2.3", null, null);
		String userId = AuditItFixture.oid();
		recorder.succeeded(userId, "alice@corp.com", LoginAuditRecorder.METHOD_LOCAL, AuditSource.UI);
		for (String reason : new String[]{"User.email.Found", "Incorrect.Password", "Account.disabled", "Too.Many.Login.Failures"}) {
			recorder.failed("alice@corp.com", LoginAuditRecorder.METHOD_LOCAL, reason, AuditSource.UI);
		}
		recorder.failed("bob", LoginAuditRecorder.METHOD_LDAP, "Incorrect.Password", AuditSource.UI);
		recorder.failed(null, LoginAuditRecorder.METHOD_SAML, "sso_failed", AuditSource.UI);

		UserLogs success = fixture.events(AuditAction.LOGIN).get(0);
		assertThat(success.getOutcome()).isEqualTo("SUCCESS");
		assertThat(success.getUserId()).isEqualTo(userId);
		assertThat(success.getSourceIp()).isEqualTo("10.1.2.3");
		assertThat(success.getSource()).isEqualTo("UI");
		assertThat(success.getContext()).containsEntry("login_method", "local");
		assertThat(success.getSchemaVersion()).isEqualTo(2);
		assertThat(success.getEventId()).isNotBlank();
		assertThat(success.getOperationId()).isNotBlank();

		var failures = fixture.events(AuditAction.LOGIN).stream().filter(e -> "FAILED".equals(e.getOutcome())).toList();
		assertThat(failures).hasSize(6);
		assertThat(failures).extracting(UserLogs::getReason)
				.containsExactly("User.email.Found", "Incorrect.Password", "Account.disabled", "Too.Many.Login.Failures", "Incorrect.Password", "sso_failed");
		assertThat(failures).allSatisfy(e -> {
			assertThat(e.getUserId()).isEqualTo(AuditValues.UNAUTHENTICATED);
			assertThat(e.getSourceIp()).isEqualTo("10.1.2.3");
		});
		assertThat(failures.get(0).getAttemptedAccount()).isEqualTo("a***e@corp.com").doesNotContain("alice");
		assertThat(failures.get(4).getContext()).containsEntry("login_method", "ldap");
		assertThat(failures.get(5).getAttemptedAccount()).isNull();
	}

	@Test
	@DisplayName("access code 换 token 成功时按 userId 回填可读标识，操作人与对象不再只有内部 ID")
	void should_resolve_readable_identity_for_access_code_success() {
		// TAP-13018 F-13：登录成功事件回读 User 补 username/email；查不到用户时退回 null 并由页面显示替代值
		String userId = AuditItFixture.oid();
		LoginAuditRecorder withUser = new LoginAuditRecorder(fixture.auditEventService, null);
		withUser.succeeded(userId, LoginAuditRecorder.METHOD_ACCESS_CODE, AuditSource.OPENAPI);

		UserLogs event = fixture.events(AuditAction.API_AUTHENTICATE).get(0);
		assertThat(event.getOutcome()).isEqualTo("SUCCESS");
		assertThat(event.getUserId()).isEqualTo(userId);
		assertThat(event.getObjectId()).isEqualTo(userId);
		assertThat(event.getSource()).isEqualTo("OPENAPI");
		assertThat(event.getContext()).containsEntry("login_method", "access_code");
	}

	@Test
	@DisplayName("携带无效凭据的未登录请求记为 OpenAPI 认证失败且 5 分钟内去重；匿名请求不记录")
	void should_record_openapi_invalid_credential_but_not_anonymous() {
		StaticListableBeanFactory beans = new StaticListableBeanFactory();
		beans.addBean("audit", fixture.auditEventService);
		AccessFailureAuditor auditor = new AccessFailureAuditor(beans.getBeanProvider(AuditEventService.class));

		MockHttpServletRequest anonymous = AuditWebContext.begin("", "10.0.0.8", null, null);
		auditor.onBizException("NotLogin", anonymous);
		assertThat(fixture.events).isEmpty();
		AuditWebContext.end();

		MockHttpServletRequest sdk = new MockHttpServletRequest("GET", "/api/Task/abc");
		sdk.setRemoteAddr("10.0.0.9");
		sdk.addHeader("Authorization", "Bearer expired-token-1234567890");
		sdk.addHeader("User-Agent", "python-requests/2.31");
		AuditRequestContext.set(new AuditContextFilter("").build(sdk));
		auditor.onBizException("NotLogin", sdk);
		auditor.onBizException("NotLogin", sdk);

		UserLogs event = fixture.only(AuditAction.API_AUTHENTICATE);
		assertThat(event.getOutcome()).isEqualTo("FAILED");
		assertThat(event.getSource()).isEqualTo("OPENAPI");
		assertThat(event.getReason()).isEqualTo("NotLogin");
		assertThat(event.getUserId()).isEqualTo(AuditValues.UNAUTHENTICATED);
		assertThat(event.getCredentialRef()).isEqualTo("***7890");
		assertThat(event.getObjectName()).isEqualTo("/api/Task");
	}
}
