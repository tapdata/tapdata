package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.module.dto.ServingIndexLandingWorkList;
import com.tapdata.tm.module.dto.ServingIndexTargetOutcome;
import com.tapdata.tm.module.dto.UnresolvedServingIndexTarget;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-1 · TM 落地编排服务（{@link ServingIndexLandingService}）。
 *
 * <p>导入落完连接与 Module 之后，本服务把「这批 Module 的索引声明」翻译成 {@code (目标连接, 集合)} 工作项
 * ——目标连接只从 conMap 拿（<b>ADR-0002</b>）。聚合口径的用例在 {@code ServingIndexLandingTargetsTest}；
 * 这里钉的是服务层契约：<b>不吞</b>落不了地的记录、空入参不炸。</p>
 */
class ServingIndexLandingServiceTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	private ServingIndexLandingService service;
	private ServingIndexService servingIndexService;
	private ServingIndexRendezvous rendezvous;
	private UserDetail user;
	/** P3-5 的「响亮」是有级别的：日志级别本身就是契约，故这里真去接日志、不靠肉眼看控制台。 */
	private ListAppender<ILoggingEvent> logs;

	@BeforeEach
	void setUp() {
		servingIndexService = org.mockito.Mockito.mock(ServingIndexService.class);
		rendezvous = org.mockito.Mockito.mock(ServingIndexRendezvous.class);
		org.mockito.Mockito.when(rendezvous.newToken()).thenReturn("tm-si-landing:t1", "tm-si-landing:t2",
				"tm-si-landing:t3", "tm-si-landing:t4");
		// 默认：读回超时——想验执行路径的用例各自改掉它
		org.mockito.Mockito.when(rendezvous.await(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(ServingIndexReadback.timeout());
		service = new ServingIndexLandingService(servingIndexService, rendezvous, 10L, 10L);
		user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
				"accessCode", false, false, false, false,
				Collections.singletonList(new SimpleGrantedAuthority("role")));
		logs = new ListAppender<>();
		logs.start();
		serviceLogger().addAppender(logs);
	}

	@AfterEach
	void tearDown() {
		serviceLogger().detachAppender(logs);
	}

	private static ch.qos.logback.classic.Logger serviceLogger() {
		return (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ServingIndexLandingService.class);
	}

	private List<String> loggedAt(Level level) {
		List<String> messages = new ArrayList<>();
		for (ILoggingEvent event : logs.list) {
			if (event.getLevel() == level) {
				messages.add(event.getFormattedMessage());
			}
		}
		return messages;
	}

	private static ModulesDto api(String name, String connectionId, String tableName, ServingIndex... indexes) {
		ModulesDto module = new ModulesDto();
		module.setId(new ObjectId());
		module.setName(name);
		module.setConnectionId(connectionId);
		module.setTableName(tableName);
		module.setServingIndexes(new ArrayList<>(Arrays.asList(indexes)));
		return module;
	}

	private static ServingIndex idx(String name, String field, Boolean asc) {
		return new ServingIndex(name, null,
				new ArrayList<>(Collections.singletonList(new ServingIndexField(field, asc))));
	}

	private static Map<String, DataSourceConnectionDto> conMap() {
		DataSourceConnectionDto target = new DataSourceConnectionDto();
		target.setId(new ObjectId(CONNECTION_ID));
		target.setName("MDM-生产库");
		Map<String, DataSourceConnectionDto> map = new LinkedHashMap<>();
		map.put(CONNECTION_ID, target);
		return map;
	}

	@Test
	@DisplayName("产出 (连接,集合) 工作项，连接取自 conMap")
	void buildsWorkListFromImportedModules() {
		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				conMap(), user);

		assertEquals(1, work.getTargets().size());
		assertEquals(CONNECTION_ID, work.getTargets().get(0).getConnectionId());
		assertEquals("CUSTOMER", work.getTargets().get(0).getTableName());
		assertEquals(1, work.declaredCount());
		assertTrue(work.getUnresolved().isEmpty());
	}

	@Test
	@DisplayName("conMap 未命中的声明进 unresolved 桶——不静默跳过（否则等于「部署报成功、索引没建」）")
	void carriesUnresolvedInsteadOfDroppingThem() {
		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				new HashMap<>(), user);

		assertTrue(work.getTargets().isEmpty());
		assertEquals(1, work.getUnresolved().size());
		assertEquals(UnresolvedServingIndexTarget.Reason.CONNECTION_UNRESOLVED,
				work.getUnresolved().get(0).getReason());
	}

	@Test
	@DisplayName("没有 Module / 没有 conMap / 没有声明都不炸，返回空工作表")
	void toleratesEmptyInput() {
		assertTrue(service.landAfterImport(null, null, user).isEmpty());
		assertTrue(service.landAfterImport(Collections.<ModulesDto>emptyList(), conMap(), user).isEmpty());
		assertTrue(service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER")), conMap(), user).isEmpty());
	}

	@Test
	@DisplayName("多 API 共表 → 一个工作项，声明并集（去重在 planner，不在本层）")
	void unionsSameTableAcrossApis() {
		List<ModulesDto> modules = Arrays.asList(
				api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true)),
				api("查客户明细", CONNECTION_ID, "CUSTOMER", idx("b_-1", "b", false)));

		ServingIndexLandingWorkList work = service.landAfterImport(modules, conMap(), user);

		assertEquals(1, work.getTargets().size());
		assertEquals(2, work.declaredCount());
		assertEquals(Arrays.asList("查客户", "查客户明细"), work.getTargets().get(0).getSourceApis());
	}

	// ---- P3-3 幂等执行（ADR-0005 / ADR-0012 D1）----

	private static TapIndex existing(String name, String field, Boolean asc) {
		return new TapIndex().name(name).indexField(new TapIndexField().name(field).fieldAsc(asc));
	}

	private void readbackReturns(ServingIndexReadback readback) {
		org.mockito.Mockito.when(rendezvous.await(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(readback);
	}

	@Test
	@DisplayName("目标已有同字段集索引 → 不发建索引（幂等：连跑第二次全部 skip）")
	void skipsCreateWhenTargetAlreadyHasTheIndex() {
		readbackReturns(ServingIndexReadback.success(
				Collections.singletonList(existing("随便什么名", "a", true))));

		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				conMap(), user);

		ServingIndexTargetOutcome outcome = work.getOutcomes().get(0);
		assertEquals(0, outcome.getPlan().getCreate().size());
		assertEquals(1, outcome.getPlan().getSkip().size());
		assertTrue(outcome.isSucceeded());
		org.mockito.Mockito.verify(servingIndexService, org.mockito.Mockito.never()).sendCreateIndexes(
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
	}

    @Test
	@DisplayName("目标缺索引 → 只把「将创建」那一桶发下去，已有的不重发")
	void createsOnlyTheMissingBucket() {
		// 第二次读回 = 建后复核，故要带上刚建成的 b_-1（真实库就是这样）
		readbackReturns(ServingIndexReadback.success(
						Collections.singletonList(existing("a_1", "a", true))),
				ServingIndexReadback.success(Arrays.asList(
						existing("a_1", "a", true), existing("b_-1", "b", false))));
		org.mockito.Mockito.when(rendezvous.awaitCreateAck(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(ServingIndexCreateAck.of(Collections.singletonList("b_-1"), Collections.emptyList()));

		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER",
						idx("a_1", "a", true), idx("b_-1", "b", false))),
				conMap(), user);

		@SuppressWarnings("unchecked")
		org.mockito.ArgumentCaptor<List<ServingIndex>> sent =
				org.mockito.ArgumentCaptor.forClass((Class) List.class);
		org.mockito.Mockito.verify(servingIndexService).sendCreateIndexes(
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("CUSTOMER"), sent.capture(),
				org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
				org.mockito.ArgumentMatchers.any());
		assertEquals(1, sent.getValue().size(), "已存在的 a_1 不该再发一次");
		assertEquals("b_-1", sent.getValue().get(0).getName());
		assertEquals(Collections.singletonList("b_-1"), work.getOutcomes().get(0).getCreated());
		assertTrue(work.getOutcomes().get(0).isSucceeded());
	}

	@Test
	@DisplayName("读回超时 → 绝不建：把「拿不到」当成「目标没有索引」就会重复建索引")
	void neverCreatesWhenReadbackFailed() {
		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				conMap(), user);

		ServingIndexTargetOutcome outcome = work.getOutcomes().get(0);
		assertTrue(outcome.getError() != null && outcome.getError().contains("timeout"), outcome.getError());

		assertTrue(outcome.getPlan() == null, "读回失败时不该有比对结果——那与「比对了、无需创建」是两回事");
		org.mockito.Mockito.verify(servingIndexService, org.mockito.Mockito.never()).sendCreateIndexes(
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
	}

	@Test
	@DisplayName("一个集合塌了不拖累其余集合——不首错即停")
	void oneFailedTargetDoesNotAbortTheRest() {
		org.mockito.Mockito.when(rendezvous.await(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(ServingIndexReadback.failed("engine boom"))
				.thenReturn(ServingIndexReadback.success(Collections.singletonList(existing("b_-1", "b", false))));

		ServingIndexLandingWorkList work = service.landAfterImport(
				Arrays.asList(
						api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true)),
						api("查订单", CONNECTION_ID, "ORDER", idx("b_-1", "b", false))),
				conMap(), user);

		assertEquals(2, work.getOutcomes().size());
		assertEquals("engine boom", work.getOutcomes().get(0).getError());
		assertTrue(work.getOutcomes().get(1).isSucceeded(), "第二个集合照常走完");
	}

	@Test
	@DisplayName("P3-4 dry-run：照样读回+比对出三桶，但一条索引都不建")
	void previewComparesButNeverCreates() {
		readbackReturns(ServingIndexReadback.success(
				Collections.singletonList(existing("a_1", "a", true))));

		com.tapdata.tm.module.dto.ServingIndexLandingReport report = service.preview(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER",
						idx("a_1", "a", true), idx("b_-1", "b", false))),
				conMap(), user);

		com.tapdata.tm.module.dto.ServingIndexTargetReport target = report.getTargets().get(0);
		assertEquals(1, target.getCreate().size(), "报告要照实说「会建 b_-1」");
		assertEquals("b_-1", target.getCreate().get(0).getName());
		assertEquals(1, target.getSkip().size());
		org.mockito.Mockito.verify(servingIndexService, org.mockito.Mockito.never()).sendCreateIndexes(
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
	}

	// ---- P3-5 错误处置（方案 §4 风险表）----

	@Test
	@DisplayName("conMap 未命中走 ERROR——warn 会淹在 CICD 日志里，和静默跳过没区别")
	void reportsUnresolvedTargetsLoudly() {
		service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				new HashMap<>(), user);

		List<String> errors = loggedAt(Level.ERROR);
		assertTrue(errors.stream().anyMatch(line -> line.contains("unresolved") && line.contains("查客户")),
				"未命中必须以 ERROR 点名到 API: " + errors);
	}

	@Test
	@DisplayName("有失败 → 末尾一行 ERROR 汇总，点名失败的 (连接,集合)")
	void summarisesFailuresAtErrorLevel() {
		service.landAfterImport(
				Arrays.asList(
						api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true)),
						api("查订单", CONNECTION_ID, "ORDER", idx("b_-1", "b", false))),
				conMap(), user);

		List<String> errors = loggedAt(Level.ERROR);
		assertTrue(errors.stream().anyMatch(line -> line.contains("summary") && line.contains("failed = 2")),
				"失败要有一处一次说清，而不是散在逐 target 日志里: " + errors);
		assertTrue(errors.stream().anyMatch(line -> line.contains("CUSTOMER") && line.contains("ORDER")),
				"汇总要逐 (连接,集合) 点名: " + errors);
	}

	@Test
	@DisplayName("全部落地 → 汇总走 INFO，不制造假警报")
	void summarisesCleanRunAtInfoLevel() {
		readbackReturns(ServingIndexReadback.success(
				Collections.singletonList(existing("随便什么名", "a", true))));

		service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				conMap(), user);

		assertTrue(loggedAt(Level.ERROR).isEmpty(), "干净的落地不该有 ERROR: " + loggedAt(Level.ERROR));
		assertTrue(loggedAt(Level.INFO).stream()
						.anyMatch(line -> line.contains("summary") && line.contains("succeeded = 1")),
				loggedAt(Level.INFO).toString());
	}

	@Test
	@DisplayName("目标账号无 DDL 权限 → 汇总里说人话，而不是把驱动原文丢给人")
	void namesMissingDdlPrivilegeInsteadOfRawDriverText() {
		readbackReturns(ServingIndexReadback.success(Collections.emptyList()));
		org.mockito.Mockito.when(rendezvous.awaitCreateAck(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(ServingIndexCreateAck.of(Collections.emptyList(), Collections.singletonList(
						"a_1: Command failed with error 13 (Unauthorized): 'not authorized on mdm to execute "
								+ "command { createIndexes: \"CUSTOMER\" }'")));

		service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				conMap(), user);

		List<String> errors = loggedAt(Level.ERROR);
		assertTrue(errors.stream().anyMatch(line -> line.contains("privilege")),
				"要明确报「需 DDL 权限」（方案 §4），而不是裸抛驱动错误: " + errors);
		assertTrue(errors.stream().anyMatch(line -> line.contains("not authorized on mdm")),
				"驱动原文仍要留着——排查靠它: " + errors);
	}

	// ---- 建后复核（P3-3 补强，2026-08-02 实机现场：连接器吞 85 后照报成功）----

	/** 首次读回（比对用）与建后复核那次读回，按调用次序分别打桩。 */
	private void readbackReturns(ServingIndexReadback first, ServingIndexReadback second) {
		org.mockito.Mockito.when(rendezvous.await(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(first).thenReturn(second);
	}

	private void createAckReturns(ServingIndexCreateAck ack) {
		org.mockito.Mockito.when(rendezvous.awaitCreateAck(org.mockito.ArgumentMatchers.anyString(),
						org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong()))
				.thenReturn(ack);
	}

	private ServingIndexLandingWorkList landOneDescendingDeclaration() {
		return service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("b_-1", "b", false))),
				conMap(), user);
	}

	@Test
	@DisplayName("建完再读回一次确认真建上了")
	void verifiesCreatedIndexesByReadingThemBack() {
		readbackReturns(ServingIndexReadback.success(Collections.emptyList()),
				ServingIndexReadback.success(Collections.singletonList(existing("b_-1", "b", false))));
		createAckReturns(ServingIndexCreateAck.of(Collections.singletonList("b_-1"), Collections.emptyList()));

		ServingIndexTargetOutcome outcome = landOneDescendingDeclaration().getOutcomes().get(0);

		assertEquals(Collections.singletonList("b_-1"), outcome.getCreated());
		assertTrue(outcome.getFailed().isEmpty());
		assertTrue(outcome.isCreateVerified());
		assertTrue(outcome.isSucceeded());
		org.mockito.Mockito.verify(servingIndexService, org.mockito.Mockito.times(2)).sendQueryIndexes(
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("CUSTOMER"),
				org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
				org.mockito.ArgumentMatchers.any());
	}

	@Test
	@DisplayName("连接器报「建了」但库里没有 → 判失败，不跟着连接器一起撒谎")
	void catchesSwallowedConflictWhenTheIndexIsNotActuallyThere() {
		// 现场就是这样：声明降序、旧连接器按升序建、撞同键异名 → 85 → 吞掉 → 回执报 created
		readbackReturns(ServingIndexReadback.success(Collections.emptyList()),
				ServingIndexReadback.success(Collections.singletonList(existing("b_1", "b", true))));
		createAckReturns(ServingIndexCreateAck.of(Collections.singletonList("b_-1"), Collections.emptyList()));

		ServingIndexTargetOutcome outcome = landOneDescendingDeclaration().getOutcomes().get(0);

		assertTrue(outcome.getCreated().isEmpty(), "复核没找到就不能算建成: " + outcome.getCreated());
		assertEquals(1, outcome.getFailed().size());
		assertTrue(outcome.getFailed().get(0).startsWith("b_-1:"), outcome.getFailed().get(0));
		assertTrue(outcome.isCreateVerified());
		assertFalse(outcome.isSucceeded());
	}

	@Test
	@DisplayName("一条都没建就不多跑一次读回——复核只为「说建了」的那些")
	void skipsVerificationWhenNothingWasCreated() {
		readbackReturns(ServingIndexReadback.success(
				Collections.singletonList(existing("随便什么名", "b", false))));

		landOneDescendingDeclaration();

		org.mockito.Mockito.verify(servingIndexService, org.mockito.Mockito.times(1)).sendQueryIndexes(
				org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("CUSTOMER"),
				org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
				org.mockito.ArgumentMatchers.any());
	}

	@Test
	@DisplayName("复核那次读回本身失败 → 不倒打一耙判失败，但也不声称已核实")
	void keepsCreatedButUnverifiedWhenVerificationReadbackFails() {
		readbackReturns(ServingIndexReadback.success(Collections.emptyList()),
				ServingIndexReadback.timeout());
		createAckReturns(ServingIndexCreateAck.of(Collections.singletonList("b_-1"), Collections.emptyList()));

		ServingIndexTargetOutcome outcome = landOneDescendingDeclaration().getOutcomes().get(0);

		assertEquals(Collections.singletonList("b_-1"), outcome.getCreated());
		assertTrue(outcome.getFailed().isEmpty(), "拿不到复核结果不等于没建成");
		assertFalse(outcome.isCreateVerified());
	}
}
