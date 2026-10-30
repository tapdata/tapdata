package com.tapdata.tm.audit;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.audit.query.AuditEventVo;
import com.tapdata.tm.audit.query.AuditQueryService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.userLog.controller.UserLogController;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.param.AddUserLogParam;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.userLog.service.UserLogServiceImpl;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** L2：升级前的用户操作记录兼容展示且不补齐缺失字段；客户端不能写入审计记录（需求 US-13、BR-13）。 */
@Timeout(10)
class AuditHistoryCompatIT {
	@Test
	@DisplayName("历史记录标记为 legacy，保留原描述，缺失的来源 IP、结果、Operation ID 保持为空由页面显示历史说明")
	void should_show_legacy_records_with_unavailable_fields() throws Exception {
		UserLogServiceImpl userLogService = mock(UserLogServiceImpl.class);
		when(userLogService.renderLegacyMessage(any(UserLogDto.class), any(Locale.class))).thenReturn("hardy 启动了任务 orders");
		AuditQueryService service = new AuditQueryService(new AuditItFixture().mongoTemplate, userLogService);
		UserLogs legacy = new UserLogs();
		legacy.setId(new ObjectId());
		legacy.setType("userOperation");
		legacy.setModular("migration");
		legacy.setOperation("start");
		legacy.setParameter1("orders");
		legacy.setUsername("hardy");
		legacy.setUserId("u1");
		legacy.setCreateAt(new Date(1_690_000_000_000L));
		Method toVo = AuditQueryService.class.getDeclaredMethod("toVo", UserLogs.class, Locale.class);
		toVo.setAccessible(true);
		AuditEventVo vo = (AuditEventVo) toVo.invoke(service, legacy, Locale.SIMPLIFIED_CHINESE);
		assertThat(vo.isLegacy()).isTrue();
		assertThat(vo.getLegacyMessage()).isEqualTo("hardy 启动了任务 orders");
		assertThat(vo.getObjectName()).isEqualTo("orders");
		assertThat(vo.getTimestamp()).isEqualTo(new Date(1_690_000_000_000L));
		assertThat(vo.getSourceIp()).isNull();
		assertThat(vo.getOutcome()).isNull();
		assertThat(vo.getOperationId()).isNull();
		assertThat(vo.getSource()).isNull();
	}

	@Test
	@DisplayName("POST /api/UserLogs 拒绝客户端写入伪造的审计记录")
	void should_reject_client_written_user_logs() {
		UserLogService userLogService = mock(UserLogService.class);
		UserLogController controller = new UserLogController(userLogService, new ModuleAccess(mock(PermissionService.class), mock(SettingsService.class)));
		AddUserLogParam param = new AddUserLogParam();
		param.setModular("connection");
		param.setOperation("delete");
		param.setParameter1("fake");
		assertThatThrownBy(() -> controller.add(param))
				.isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("Audit.Log.ReadOnly"));
		org.mockito.Mockito.verifyNoInteractions(userLogService);
	}
}
