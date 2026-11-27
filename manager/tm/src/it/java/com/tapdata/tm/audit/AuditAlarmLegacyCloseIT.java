package com.tapdata.tm.audit;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.controller.AlarmController;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmEventQueryService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.service.UserLogService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.aop.aspectj.annotation.AnnotationAwareAspectJAutoProxyCreator;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L2: legacy {@code POST /api/alarm/close} through the real AuditAspect, AlarmController, AlarmEventQueryService and
 * ModuleAccess; the audit record must only report alarms that were actually closed (TAP-13349 review).
 * Alarm storage, task/inspect lookups and permission storage are boundaries; audit documents are checked out of band.
 */
@Timeout(10)
class AuditAlarmLegacyCloseIT {
	private static final String VISIBLE_TASK = "64d000000000000000000001";
	private static final String OTHER_TASK = "64d000000000000000000002";

	private final AuditItFixture fixture = new AuditItFixture();
	private final List<AlarmInfo> alarmData = new ArrayList<>();
	private final List<String> closed = new ArrayList<>();
	private GenericApplicationContext context;
	private AlarmController controller;
	private UserDetail member;

	@BeforeEach
	void setUp() {
		member = AuditItFixture.user("member", "member@corp.com", "username_login");
		AlarmService alarms = mock(AlarmService.class);
		when(alarms.query(any(Query.class))).thenAnswer(i -> {
			Map<String, Object> filter = i.<Query>getArgument(0).getQueryObject();
			return alarmData.stream().filter(a -> matches(filter, a)).collect(Collectors.toList());
		});
		org.mockito.Mockito.doAnswer(i -> {
			closed.addAll(List.of(i.<String[]>getArgument(0)));
			return null;
		}).when(alarms).close(any(), any());
		TaskService tasks = mock(TaskService.class);
		TaskDto task = new TaskDto();
		task.setId(new ObjectId(VISIBLE_TASK));
		when(tasks.findAllDto(any(Query.class), eq(member))).thenReturn(List.of(task));
		InspectService inspects = mock(InspectService.class);
		when(inspects.findAllDto(any(Query.class), eq(member))).thenReturn(List.of());
		PermissionService permissions = mock(PermissionService.class);
		when(permissions.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ALARM_EVENT_ACK_CLOSE, "member")).thenReturn(true);
		ModuleAccess access = new ModuleAccess(permissions, mock(SettingsService.class));
		AlarmEventQueryService queryService = new AlarmEventQueryService(mock(MongoTemplate.class), tasks, inspects, alarms,
				mock(NotificationDeliveryService.class), access, mock(UserLogService.class));

		context = new GenericApplicationContext();
		context.registerBean(AnnotationAwareAspectJAutoProxyCreator.class);
		context.registerBean(AuditAspect.class, () -> new AuditAspect(fixture.auditEventService, fixture.objectResolver, fixture.registry));
		context.registerBean(LegacyAlarmController.class, () -> {
			LegacyAlarmController c = new LegacyAlarmController(member);
			c.setAlarmService(alarms);
			c.setUserLogService(mock(UserLogService.class));
			c.setModuleAccess(access);
			c.setAlarmEventQueryService(queryService);
			return c;
		});
		context.refresh();
		controller = context.getBean(LegacyAlarmController.class);
		AuditWebContext.begin("", "10.7.0.1", null, member);
	}

	@AfterEach
	void tearDown() {
		AuditWebContext.end();
		context.close();
	}

	@Test
	@DisplayName("TAP-13349 旧关闭接口审计：只把实际关闭的告警记为成功，不可见与不存在的告警记为失败（部分成功）")
	void should_audit_only_actually_closed_alarms() {
		String visible = alarm(VISIBLE_TASK);
		String hidden = alarm(OTHER_TASK);
		String missing = new ObjectId().toHexString();

		controller.close(new String[]{visible, hidden, missing});

		assertThat(closed).containsExactly(visible);
		UserLogs event = fixture.only(AuditAction.ALARM_CLOSE);
		assertThat(event.getOutcome()).isEqualTo("PARTIAL");
		assertThat(event.getBatch().getTotal()).isEqualTo(3);
		assertThat(event.getBatch().getSuccess()).isEqualTo(1);
		assertThat(event.getBatch().getFailed()).isEqualTo(2);
		Map<String, AuditOutcome> outcomes = event.getBatch().getItems().stream()
				.collect(Collectors.toMap(AuditBatchItem::getObjectId, AuditBatchItem::getOutcome));
		assertThat(outcomes).containsEntry(visible, AuditOutcome.SUCCESS)
				.containsEntry(hidden, AuditOutcome.FAILED)
				.containsEntry(missing, AuditOutcome.FAILED);
	}

	@Test
	@DisplayName("TAP-13349 旧关闭接口审计：请求的告警全部不可见时不关闭任何告警，审计记为失败而非成功")
	void should_audit_failure_when_nothing_closed() {
		String hidden = alarm(OTHER_TASK);

		controller.close(new String[]{hidden});

		assertThat(closed).isEmpty();
		UserLogs event = fixture.only(AuditAction.ALARM_CLOSE);
		assertThat(event.getOutcome()).isEqualTo("FAILED");
		assertThat(event.getBatch().getFailed()).isEqualTo(1);
		assertThat(event.getObjectId()).isEqualTo(hidden);
	}

	private String alarm(String taskId) {
		AlarmInfo info = new AlarmInfo();
		info.setId(new ObjectId());
		info.setTaskId(taskId);
		alarmData.add(info);
		return info.getId().toHexString();
	}

	/** Minimal Mongo filter evaluation for the operators used by the close query. */
	@SuppressWarnings("unchecked")
	private static boolean matches(Map<String, Object> filter, AlarmInfo alarm) {
		for (Map.Entry<String, Object> e : filter.entrySet()) {
			Object cond = e.getValue();
			boolean ok = switch (e.getKey()) {
				case "$and" -> ((Collection<Map<String, Object>>) cond).stream().allMatch(c -> matches(c, alarm));
				case "$or" -> ((Collection<Map<String, Object>>) cond).stream().anyMatch(c -> matches(c, alarm));
				default -> {
					Object value = switch (e.getKey()) {
						case "_id" -> alarm.getId();
						case "taskId" -> alarm.getTaskId();
						case "inspectId" -> alarm.getInspectId();
						default -> throw new IllegalArgumentException("unexpected alarm field " + e.getKey());
					};
					if (cond instanceof Map<?, ?> ops && ops.containsKey("$in")) {
						yield ((Collection<?>) ops.get("$in")).stream().anyMatch(v -> Objects.equals(v, value));
					}
					yield Objects.equals(cond, value);
				}
			};
			if (!ok) {
				return false;
			}
		}
		return true;
	}

	/** Controller under test with the login user fixed, as LoginUserResolver would resolve it for the request. */
	static class LegacyAlarmController extends AlarmController {
		private final UserDetail user;

		LegacyAlarmController() {
			this(null);
		}

		LegacyAlarmController(UserDetail user) {
			this.user = user;
		}

		@Override
		public UserDetail getLoginUser() {
			return user;
		}
	}
}
