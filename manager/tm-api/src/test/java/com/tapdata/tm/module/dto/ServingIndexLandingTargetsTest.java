package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import io.tapdata.entity.schema.TapIndex;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-1 · 落地工作项聚合（{@link ServingIndexLandingTargets}）。
 *
 * <p>把「刚导入的一批 Module」翻译成 {@code (目标连接, 集合)} 工作项：目标连接经 <b>conMap</b> 解析
 * （<b>ADR-0002</b>——索引本体只落用户库，连接只能从 conMap 拿，TM 侧不碰 {@code MongoTemplate}），
 * 同 {@code (连接, 集合)} 的多个 API 声明<b>并集</b>成一项。</p>
 *
 * <p><b>去重不在这里</b>：并集后的重复声明由 {@link ServingIndexLandingPlanner} 按签名合并
 * （P3-2 已实现且调用方绕不过），本层只负责「归并到同一桶」，不另写一份判等。</p>
 */
class ServingIndexLandingTargetsTest {

	private static ServingIndex idx(String name, ServingIndexField... fields) {
		return new ServingIndex(name, null, new ArrayList<>(Arrays.asList(fields)));
	}

	private static ServingIndexField f(String field, Boolean asc) {
		return new ServingIndexField(field, asc);
	}

	private static ServingIndex uncollected(String name, ServingIndexField... fields) {
		ServingIndex index = idx(name, fields);
		index.setCollected(false);
		return index;
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

	private static DataSourceConnectionDto connection(String id, String name) {
		DataSourceConnectionDto dto = new DataSourceConnectionDto();
		dto.setId(new ObjectId(id));
		dto.setName(name);
		return dto;
	}

	private static final String OLD_ID = "64b7e1f4c9e77a0001aa0001";
	private static final String NEW_ID = "64b7e1f4c9e77a0001bb0002";

	private static Map<String, DataSourceConnectionDto> conMap(String oldId, DataSourceConnectionDto target) {
		Map<String, DataSourceConnectionDto> map = new LinkedHashMap<>();
		map.put(oldId, target);
		return map;
	}

	@Test
	@DisplayName("声明经 conMap 解析到目标连接：工作项带目标环境的连接 id/名，而非导出侧的旧 id")
	void resolvesTargetConnectionThroughConMap() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", OLD_ID, "CUSTOMER", idx("a_1", f("a", true)))),
				conMap(OLD_ID, target));

		assertTrue(work.getUnresolved().isEmpty());
		assertEquals(1, work.getTargets().size());
		ServingIndexLandingTarget landing = work.getTargets().get(0);
		assertEquals(NEW_ID, landing.getConnectionId(), "落地必须打到目标环境的连接，不是导出包里的旧 id");
		assertEquals("MDM-生产库", landing.getConnectionName());
		assertEquals("CUSTOMER", landing.getTableName());
		assertEquals(1, landing.getDeclared().size());
		assertEquals(Collections.singletonList("查客户"), landing.getSourceApis());
	}

	@Test
	@DisplayName("同 (连接,集合) 的多个 API 并成一项；重复声明留给 planner 去重，本层不判等")
	void unionsDeclarationsOfSameConnectionAndTable() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Arrays.asList(
						api("查客户", OLD_ID, "CUSTOMER", idx("源环境名甲", f("a", true))),
						api("查客户明细", OLD_ID, "CUSTOMER", idx("源环境名乙", f("a", true)), idx("b_-1", f("b", false)))),
				conMap(OLD_ID, target));

		assertEquals(1, work.getTargets().size());
		ServingIndexLandingTarget landing = work.getTargets().get(0);
		assertEquals(3, landing.getDeclared().size(), "本层只做并集——同字段集的两条都还在");
		assertEquals(Arrays.asList("查客户", "查客户明细"), landing.getSourceApis());

		// 交给 P3-2 的 planner，才落到「一条创建」：去重口径只有一份，在 planner 里。
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(landing.getDeclared(), Collections.<TapIndex>emptyList());
		assertEquals(2, plan.getCreate().size(), "同字段集的两条声明合成一条 a_1，另加 b_-1");
	}

	@Test
	@DisplayName("同连接不同集合 → 两项（索引本体按集合建，不能混桶）")
	void splitsByTable() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Arrays.asList(
						api("查客户", OLD_ID, "CUSTOMER", idx("a_1", f("a", true))),
						api("查订单", OLD_ID, "ORDER", idx("a_1", f("a", true)))),
				conMap(OLD_ID, target));

		assertEquals(2, work.getTargets().size());
		assertEquals("CUSTOMER", work.getTargets().get(0).getTableName());
		assertEquals("ORDER", work.getTargets().get(1).getTableName());
	}

	@Test
	@DisplayName("只存着未勾选项的 API 不产生工作项——它没有声明，没什么可建（ADR-0011 D2）")
	void uncollectedEntriesProduceNoTarget() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", OLD_ID, "CUSTOMER", uncollected("a_1", f("a", true)))),
				conMap(OLD_ID, target));

		assertTrue(work.getTargets().isEmpty());
		assertTrue(work.getUnresolved().isEmpty());
	}

	@Test
	@DisplayName("未勾选项被剔除，只有声明项进工作项")
	void keepsOnlyDeclarations() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", OLD_ID, "CUSTOMER",
						uncollected("b_1", f("b", true)), idx("a_1", f("a", true)))),
				conMap(OLD_ID, target));

		assertEquals(1, work.getTargets().size());
		List<ServingIndex> declared = work.getTargets().get(0).getDeclared();
		assertEquals(1, declared.size());
		assertEquals("a_1", declared.get(0).getName());
	}

	@Test
	@DisplayName("没有声明的 API 即便连接解析不出来也不报——没什么可落地，不该拖累导入")
	void unresolvedConnectionWithoutDeclarationsIsSilent() {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", OLD_ID, "CUSTOMER")),
				new HashMap<>());

		assertTrue(work.getTargets().isEmpty());
		assertTrue(work.getUnresolved().isEmpty());
	}

	@Test
	@DisplayName("有声明但 conMap 未命中 → 记 unresolved（P3-5 据此响亮报错），绝不猜一个连接去建")
	void reportsUnresolvedConnection() {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", OLD_ID, "CUSTOMER", idx("a_1", f("a", true)))),
				new HashMap<>());

		assertTrue(work.getTargets().isEmpty());
		assertEquals(1, work.getUnresolved().size());
		UnresolvedServingIndexTarget gap = work.getUnresolved().get(0);
		assertEquals(UnresolvedServingIndexTarget.Reason.CONNECTION_UNRESOLVED, gap.getReason());
		assertEquals("查客户", gap.getApiName());
		assertEquals(OLD_ID, gap.getConnectionId());
		assertEquals(1, gap.getIndexCount());
	}

	@Test
	@DisplayName("有声明但没有表名 → 记 unresolved，不去猜集合")
	void reportsMissingTableName() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", OLD_ID, "   ", idx("a_1", f("a", true)))),
				conMap(OLD_ID, target));

		assertTrue(work.getTargets().isEmpty());
		assertEquals(1, work.getUnresolved().size());
		assertEquals(UnresolvedServingIndexTarget.Reason.TABLE_NAME_MISSING,
				work.getUnresolved().get(0).getReason());
	}

	@Test
	@DisplayName("Module 的 connectionId 已被 batchImport 改写成目标 id 时，仍按 conMap 的值命中")
	void resolvesWhenConnectionIdAlreadyRemapped() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");
		// modulesService.batchImport → updateConnectionIds 会就地把 connectionId 改成目标 id，
		// 而 conMap 的键仍是导出侧旧 id：按键查不到，得按值里的目标 id 兜住。
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", NEW_ID, "CUSTOMER", idx("a_1", f("a", true)))),
				conMap(OLD_ID, target));

		assertEquals(1, work.getTargets().size());
		assertSame(target, work.getTargets().get(0).getConnection());
	}

	@Test
	@DisplayName("connectionId 缺失时按 connection / dataSource 兜底解析（与 ServingIndexLoadService 同序）")
	void fallsBackToConnectionAndDataSource() {
		DataSourceConnectionDto target = connection(NEW_ID, "MDM-生产库");

		ModulesDto viaConnection = api("查客户", null, "CUSTOMER", idx("a_1", f("a", true)));
		viaConnection.setConnection(new ObjectId(OLD_ID));
		assertEquals(1, ServingIndexLandingTargets.from(
				Collections.singletonList(viaConnection), conMap(OLD_ID, target)).getTargets().size());

		ModulesDto viaDataSource = api("查客户", null, "CUSTOMER", idx("a_1", f("a", true)));
		viaDataSource.setDataSource(OLD_ID);
		assertEquals(1, ServingIndexLandingTargets.from(
				Collections.singletonList(viaDataSource), conMap(OLD_ID, target)).getTargets().size());
	}

	@Test
	@DisplayName("空入参/空 conMap 不炸")
	void toleratesEmptyInput() {
		assertTrue(ServingIndexLandingTargets.from(null, null).getTargets().isEmpty());
		assertTrue(ServingIndexLandingTargets.from(Collections.emptyList(), null).getUnresolved().isEmpty());
		assertTrue(ServingIndexLandingTargets.from(Collections.singletonList(new ModulesDto()), null)
				.getTargets().isEmpty());
	}
}
