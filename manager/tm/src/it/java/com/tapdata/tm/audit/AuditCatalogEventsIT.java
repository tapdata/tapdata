package com.tapdata.tm.audit;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** L2：真实 AuditAspect 代理接口方法，验证同步成功/失败、UI 与 OpenAPI 同口径、删除保留对象名与高风险读取（需求 US-03、11.2）。 */
@Timeout(10)
class AuditCatalogEventsIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private GenericApplicationContext context;
	private AuditDemoController controller;

	@BeforeEach
	void setUp() {
		context = AuditWebContext.aspectContext(fixture);
		controller = context.getBean(AuditDemoController.class);
	}

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
		context.close();
	}

	@Test
	@DisplayName("同一业务操作来自 UI 与 OpenAPI 时 Action、对象一致，仅来源与身份不同")
	void should_use_same_action_for_ui_and_openapi() {
		String taskId = AuditItFixture.oid();
		fixture.put("Task", new Document("_id", new ObjectId(taskId)).append("name", "orders-sync"));
		AuditWebContext.begin("", "10.4.0.1", null, AuditItFixture.user("u-ui", "ui@corp.com", "username_login"));
		controller.start(taskId);
		AuditWebContext.end();
		AuditWebContext.begin("", "10.4.0.2", null, AuditItFixture.user("u-api", "sdk@corp.com", "access_code"));
		controller.start(taskId);

		assertThat(fixture.events(AuditAction.TASK_START)).hasSize(2).allSatisfy(e -> {
			assertThat(e.getAction()).isEqualTo("task.start");
			assertThat(e.getObjectType()).isEqualTo("task");
			assertThat(e.getObjectId()).isEqualTo(taskId);
			assertThat(e.getObjectName()).isEqualTo("orders-sync");
		});
		assertThat(fixture.events).extracting(UserLogs::getSource).containsExactly("UI", "OPENAPI");
		assertThat(fixture.events).extracting(UserLogs::getUserId).containsExactly("u-ui", "u-api");
	}

	@Test
	@DisplayName("同步操作只产生一条最终事件：成功、抛出业务异常与返回失败码均记录，失败原因为安全化错误码")
	void should_record_success_and_failure_for_sync_operations() {
		AuditWebContext.begin("", "10.4.0.3", null, AuditItFixture.user("u1", "a@corp.com", "username_login"));
		controller.create("orders", Map.of("name", "orders"));
		assertThatThrownBy(() -> controller.update(AuditItFixture.oid(), Map.of())).isInstanceOf(BizException.class);
		controller.softFail(AuditItFixture.oid());

		UserLogs created = fixture.only(AuditAction.TASK_CREATE);
		assertThat(created.getOutcome()).isEqualTo("SUCCESS");
		assertThat(created.getPhase()).isEqualTo("SINGLE");
		assertThat(created.getObjectId()).isEqualTo("64b000000000000000000001");
		assertThat(created.getObjectName()).isEqualTo("orders");
		UserLogs failed = fixture.only(AuditAction.DATASOURCE_UPDATE);
		assertThat(failed.getOutcome()).isEqualTo("FAILED");
		assertThat(failed.getReason()).isEqualTo("Datasource.NotFound");
		UserLogs soft = fixture.only(AuditAction.USER_UPDATE);
		assertThat(soft.getOutcome()).isEqualTo("FAILED");
		assertThat(soft.getReason()).isEqualTo("Account.disabled");
		assertThat(fixture.events).hasSize(3);
	}

	@Test
	@DisplayName("删除操作在执行前取得对象名，删除后仍可识别原对象；强制停止按参数记为独立动作")
	void should_keep_object_name_after_delete() {
		String id = AuditItFixture.oid();
		fixture.put("Connections", new Document("_id", new ObjectId(id)).append("name", "mysql-prod"));
		AuditWebContext.begin("", "10.4.0.4", null, AuditItFixture.user("u1", "a@corp.com", "username_login"));
		controller.setDeleteHook(() -> fixture.collection("Connections").clear());
		controller.delete(id);
		assertThat(fixture.collection("Connections")).isEmpty();
		assertThat(fixture.only(AuditAction.DATASOURCE_DELETE).getObjectName()).isEqualTo("mysql-prod");

		controller.stop(AuditItFixture.oid(), true);
		controller.stop(AuditItFixture.oid(), false);
		assertThat(fixture.events(AuditAction.TASK_FORCE_STOP)).hasSize(1);
		assertThat(fixture.events(AuditAction.TASK_STOP)).hasSize(1);
	}

	@Test
	@DisplayName("高风险读取（日志下载）产生审计事件；内部调用经动作表达式跳过；产品接口声明了对应目录动作")
	void should_record_high_risk_reads() throws Exception {
		AuditWebContext.begin("", "10.4.0.5", null, AuditItFixture.user("u1", "a@corp.com", "username_login"));
		String taskId = AuditItFixture.oid();
		controller.downloadLog(taskId);
		assertThat(fixture.only(AuditAction.TASK_DOWNLOAD_LOG).getObjectId()).isEqualTo(taskId);
		controller.maybeInternal(true);
		assertThat(fixture.events(AuditAction.SETTINGS_UPDATE)).isEmpty();
		controller.maybeInternal(false);
		assertThat(fixture.events(AuditAction.SETTINGS_UPDATE)).hasSize(1);

		assertAudited("com.tapdata.tm.task.controller.TaskController", "start", AuditAction.TASK_START);
		assertAudited("com.tapdata.tm.task.controller.TaskController", "batchLoadTasks", AuditAction.TASK_EXPORT);
		assertAudited("com.tapdata.tm.ds.controller.DataSourceController", "delete", AuditAction.DATASOURCE_DELETE);
		assertAudited("com.tapdata.tm.monitoringlogs.controller.MonitoringLogsController", "export", AuditAction.TASK_DOWNLOAD_LOG);
		assertAudited("com.tapdata.tm.inspect.controller.InspectDetailsController", "export", AuditAction.INSPECT_EXPORT_RESULT);
		assertAudited("com.tapdata.tm.previewData.controller.PreviewDataController", "preview", AuditAction.DATA_PREVIEW);
		assertAudited("com.tapdata.tm.system.api.controller.ApiDebugController", "debug", AuditAction.API_DEBUG);
		assertAudited("com.tapdata.tm.user.controller.UserController", "revealAccessCode", AuditAction.USER_VIEW_ACCESS_CODE);
		assertAudited("com.tapdata.tm.application.controller.ApplicationController", "revealSecret", AuditAction.API_CLIENT_VIEW_SECRET);
		assertAudited("com.tapdata.tm.role.controller.RoleController", "delete", AuditAction.ROLE_DELETE);
		assertAudited("com.tapdata.tm.modules.controller.ModulesController", "generate", AuditAction.API_PUBLISH);
		assertAudited("com.tapdata.tm.webhook.controller.WebHookController", "create", AuditAction.WEBHOOK_CREATE);
	}

	private static void assertAudited(String className, String method, AuditAction expected) throws ClassNotFoundException {
		Method found = Arrays.stream(Class.forName(className, false, AuditCatalogEventsIT.class.getClassLoader()).getDeclaredMethods())
				.filter(m -> m.getName().equals(method) && m.isAnnotationPresent(Audited.class)).findFirst().orElse(null);
		assertThat(found).as(className + "#" + method + " 应声明审计").isNotNull();
		assertThat(found.getAnnotation(Audited.class).value()).isEqualTo(expected);
	}
}
