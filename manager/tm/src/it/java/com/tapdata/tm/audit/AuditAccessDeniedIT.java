package com.tapdata.tm.audit;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

/** L2：权限拒绝统一记录（需求 BR-14、18.5-4），已由切面按业务动作记录的拒绝不重复记录。 */
@Timeout(10)
class AuditAccessDeniedIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final AccessFailureAuditor auditor;

	AuditAccessDeniedIT() {
		StaticListableBeanFactory beans = new StaticListableBeanFactory();
		beans.addBean("audit", fixture.auditEventService);
		auditor = new AccessFailureAuditor(beans.getBeanProvider(AuditEventService.class));
	}

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
	}

	@Test
	@DisplayName("权限不足记为访问被拒绝；访问审计资源被拒记为访问审计资源失败；切面已记录时不重复")
	void should_record_not_authorized_and_audit_resource_denial() {
		UserDetail user = AuditItFixture.user(AuditItFixture.oid(), "viewer@corp.com", "username_login");
		MockHttpServletRequest request = AuditWebContext.begin("", "10.2.0.1", null, user);
		request.setRequestURI("/api/roles/abc");
		auditor.onBizException("NotAuthorized", request);
		UserLogs denied = fixture.only(AuditAction.ACCESS_DENIED);
		assertThat(denied.getOutcome()).isEqualTo("FAILED");
		assertThat(denied.getUserId()).isEqualTo(user.getUserId());
		assertThat(denied.getObjectName()).isEqualTo("/api/roles");
		AuditWebContext.end();

		MockHttpServletRequest audit = AuditWebContext.begin("", "10.2.0.1", null, user);
		audit.setRequestURI("/api/audit-events/export");
		auditor.onBizException("NotAuthorized", audit);
		assertThat(fixture.only(AuditAction.AUDIT_ACCESS_DENIED).getEventType()).isEqualTo("AUDIT");
		AuditWebContext.end();

		MockHttpServletRequest inScope = AuditWebContext.begin("", "10.2.0.1", null, user);
		AuditRequestContext.current().setRecorded(true);
		auditor.onBizException("NotAuthorized", inScope);
		assertThat(fixture.events).hasSize(2);
	}

	@Test
	@DisplayName("只被授予页面权限点的角色可以查看列表（F-01 回归：页面节点即模块权限点）")
	void should_allow_list_with_page_permission_only() throws Exception {
		com.tapdata.tm.Permission.service.PermissionService permissions = org.mockito.Mockito.mock(com.tapdata.tm.Permission.service.PermissionService.class);
		com.tapdata.tm.Settings.service.SettingsService settings = org.mockito.Mockito.mock(com.tapdata.tm.Settings.service.SettingsService.class);
		org.mockito.Mockito.when(settings.isCloud()).thenReturn(false);
		// 角色页只写入页面节点 v2_audit_log（见 init/idaas/4.25-2）
		org.mockito.Mockito.when(permissions.checkCurrentUserHasPermission("v2_audit_log", "u1")).thenReturn(true);
		org.mockito.Mockito.when(permissions.checkCurrentUserHasPermission("v2_audit_log_export", "u1")).thenReturn(false);
		com.tapdata.tm.permissions.service.ModuleAccess access = new com.tapdata.tm.permissions.service.ModuleAccess(permissions, settings);
		UserDetail user = AuditItFixture.user("u1", "auditor@corp.com", "username_login");
		access.require(user, com.tapdata.tm.permissions.constants.DataPermissionEnumsName.V2_AUDIT_LOG);
		org.junit.jupiter.api.Assertions.assertThrows(com.tapdata.tm.base.exception.BizException.class,
				() -> access.require(user, com.tapdata.tm.permissions.constants.DataPermissionEnumsName.V2_AUDIT_LOG_EXPORT));
	}
}
