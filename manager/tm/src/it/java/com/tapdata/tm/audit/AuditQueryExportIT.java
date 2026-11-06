package com.tapdata.tm.audit;

import com.tapdata.tm.audit.query.AuditCsvWriter;
import com.tapdata.tm.audit.query.AuditEventVo;
import com.tapdata.tm.audit.query.AuditOperationVo;
import com.tapdata.tm.audit.query.AuditQuery;
import com.tapdata.tm.audit.query.AuditQueryService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.service.UserLogServiceImpl;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.mongodb.core.query.Query;

import java.io.StringWriter;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** L2：审计查询条件、逻辑操作聚合、稳定排序与 CSV 导出（需求 13/14 章、US-10/11）。 */
@Timeout(10)
class AuditQueryExportIT {
	private final AuditItFixture fixture = new AuditItFixture();
	private final AuditQueryService service = new AuditQueryService(fixture.mongoTemplate, mock(UserLogServiceImpl.class));

	@Test
	@DisplayName("13.4 节全部条件组合进查询；受理与最终事件聚合为一行，显示发起人与最终结果；同秒按事件 ID 稳定排序")
	void should_filter_aggregate_and_order_stably() throws Exception {
		AuditQuery q = new AuditQuery();
		q.setStart(1_700_000_000_000L);
		q.setEnd(1_700_000_600_000L);
		q.setEventType("TASK");
		q.setUser("ops");
		q.setSource("UI");
		q.setSourceIp("10.9");
		q.setAction("task.start");
		q.setObject("orders");
		q.setOutcome("FAILED");
		q.setEventId("e1");
		q.setOperationId("op1");
		q.setKeyword("10.9.0.1");
		Method criteria = AuditQueryService.class.getDeclaredMethod("criteria", AuditQuery.class, String.class);
		criteria.setAccessible(true);
		Document where = ((org.springframework.data.mongodb.core.query.Criteria) criteria.invoke(service, q, "u1")).getCriteriaObject();
		String json = where.toJson();
		for (String field : new String[]{"eventType", "source", "action", "outcome", "eventId", "operationId", "sourceIp", "username",
				"attemptedAccount", "objectName", "objectId", "parameter1", "timestamp", "createTime", "user_id"}) {
			assertThat(json).as(field).contains("\"" + field + "\"");
		}

		Date t = new Date(1_700_000_100_000L);
		List<AuditEventVo> events = new ArrayList<>();
		events.add(vo("e2", "FINAL", "SUCCESS", "SYSTEM", new Date(t.getTime() + 5000)));
		events.add(vo("e1", "REQUEST", "ACCEPTED", "u1", t));
		AuditOperationVo op = invokeAggregate("op1|task.start", events);
		assertThat(op.getInitiator().getUserId()).isEqualTo("u1");
		assertThat(op.getOutcome()).isEqualTo("SUCCESS");
		assertThat(op.getFinalTimestamp()).isEqualTo(new Date(t.getTime() + 5000));
		assertThat(op.getEvents()).extracting(AuditEventVo::getEventId).containsExactly("e1", "e2");
		assertThat(op.isPending()).isFalse();

		List<AuditEventVo> sameSecond = new ArrayList<>(List.of(vo("b", "SINGLE", "SUCCESS", "u1", t), vo("a", "SINGLE", "SUCCESS", "u1", t)));
		assertThat(invokeAggregate("x|y", sameSecond).getEvents()).extracting(AuditEventVo::getEventId).containsExactly("a", "b");
		AuditOperationVo pending = invokeAggregate("op2|task.start", new ArrayList<>(List.of(vo("p", "REQUEST", "ACCEPTED", "u1", t))));
		assertThat(pending.isPending()).isTrue();
	}

	@Test
	@DisplayName("CSV 使用原始事件粒度并含 Event ID 与 Operation ID，公式注入被转义；超过上限拒绝导出")
	void should_export_raw_events_csv_and_audit_export() throws Exception {
		StringWriter out = new StringWriter();
		AuditCsvWriter csv = new AuditCsvWriter(out, ZoneId.of("Asia/Shanghai"), Locale.SIMPLIFIED_CHINESE);
		csv.header();
		AuditEventVo row = vo("evt-1", "REQUEST", "ACCEPTED", "u1", new Date(1_700_000_000_000L));
		row.setOperationId("op-1");
		row.setObjectName("=HYPERLINK(\"http://x\")");
		row.setChanges(List.of(AuditChange.secret("password", AuditValues.SECRET_REPLACED)));
		csv.row(row);
		String text = out.toString();
		assertThat(text).startsWith("﻿event_id,operation_id,phase,timestamp");
		assertThat(text).contains("evt-1,op-1,REQUEST,2023-11-15T06:13:20+08:00");
		assertThat(text).contains("\"'=HYPERLINK(\"\"http://x\"\")\"");
		assertThat(text).doesNotContain(",=HYPERLINK");
		assertThat(text.lines().count()).isEqualTo(2);

		when(fixture.mongoTemplate.count(any(Query.class), eq("UserLogs"))).thenReturn((long) AuditQueryService.MAX_EXPORT + 1);
		assertThatThrownBy(() -> service.export(new AuditQuery(), null, e -> {
		})).isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("Audit.Export.TooMany"));
	}

	@Test
	@DisplayName("正常查询与查看详情不写审计事件")
	void should_not_record_normal_viewing() {
		when(fixture.mongoTemplate.find(any(Query.class), eq(UserLogs.class))).thenReturn(List.of());
		service.detail("op-1", null);
		service.detail(AuditItFixture.oid(), "u1");
		when(fixture.mongoTemplate.count(any(Query.class), anyString())).thenReturn(3L);
		assertThat(service.count(new AuditQuery(), null)).isEqualTo(3L);
		assertThat(fixture.events).isEmpty();
	}

	private static AuditEventVo vo(String id, String phase, String outcome, String user, Date ts) {
		AuditEventVo vo = new AuditEventVo();
		vo.setEventId(id);
		vo.setPhase(phase);
		vo.setOutcome(outcome);
		vo.setUserId(user);
		vo.setTimestamp(ts);
		vo.setAction("task.start");
		return vo;
	}

	private static AuditOperationVo invokeAggregate(String key, List<AuditEventVo> events) throws Exception {
		Method m = AuditQueryService.class.getDeclaredMethod("aggregate", String.class, List.class);
		m.setAccessible(true);
		return (AuditOperationVo) m.invoke(null, key, events);
	}
}
