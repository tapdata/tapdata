package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-4 · dry-run 分桶报告（{@link ServingIndexLandingReports}）。
 *
 * <p>报告要能替人回答四件事：<b>会建什么 / 已经有什么 / 目标多出什么（只列不删）/ 每条是哪个 API 要的</b>，
 * 外加索引总数水位（≥50 提示、≥60 严重、≥64 预检失败——Mongo 单集合硬上限，见 <b>ADR-0008</b>）。</p>
 */
class ServingIndexLandingReportsTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	private static ServingIndexField f(String field, Boolean asc) {
		return new ServingIndexField(field, asc);
	}

	private static ServingIndex idx(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	private static TapIndex existing(String name, Boolean unique, String field, Boolean asc) {
		TapIndex index = new TapIndex().name(name).indexField(new TapIndexField().name(field).fieldAsc(asc));
		if (unique != null) {
			index.unique(unique);
		}
		return index;
	}

	private static ModulesDto api(String name, String tableName, ServingIndex... indexes) {
		ModulesDto module = new ModulesDto();
		module.setId(new ObjectId());
		module.setName(name);
		module.setConnectionId(CONNECTION_ID);
		module.setTableName(tableName);
		module.setServingIndexes(new ArrayList<>(Arrays.asList(indexes)));
		return module;
	}

	private static Map<String, DataSourceConnectionDto> conMap() {
		DataSourceConnectionDto target = new DataSourceConnectionDto();
		target.setId(new ObjectId(CONNECTION_ID));
		target.setName("MDM-生产库");
		Map<String, DataSourceConnectionDto> map = new LinkedHashMap<>();
		map.put(CONNECTION_ID, target);
		return map;
	}

	/** 走真实链路造报告：聚合 → 比对 → 记账 → 出报告，避免手搓 outcome 造出现实中不存在的形状。 */
	private static ServingIndexLandingReport reportOf(List<ModulesDto> modules, List<TapIndex> targetIndexes) {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(modules, conMap());
		for (ServingIndexLandingTarget target : work.getTargets()) {
			ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
			outcome.setConnectionId(target.getConnectionId());
			outcome.setConnectionName(target.getConnectionName());
			outcome.setTableName(target.getTableName());
			outcome.setPlan(ServingIndexLandingPlanner.plan(target.getDeclared(), targetIndexes));
			work.getOutcomes().add(outcome);
		}
		return ServingIndexLandingReports.of(work);
	}

	@Test
	@DisplayName("三桶分明：将创建 / 将跳过 / 目标多出，且每条标出是哪个 API 要的")
	void bucketsAndSourceApis() {
		ServingIndexLandingReport report = reportOf(
				Arrays.asList(
						api("查客户", "CUSTOMER", idx("a_1", null, f("a", true))),
						api("查客户明细", "CUSTOMER", idx("b_-1", null, f("b", false)))),
				Arrays.asList(existing("_id_", null, "_id", true), existing("随便什么名", null, "a", true)));

		ServingIndexTargetReport target = report.getTargets().get(0);
		assertEquals(1, target.getCreate().size());
		assertEquals("b_-1", target.getCreate().get(0).getName());
		assertEquals(Collections.singletonList("查客户明细"), target.getCreate().get(0).getSourceApis(),
				"来源 API 按签名回查——名字对不上也要能归因");

		assertEquals(1, target.getSkip().size());
		assertEquals(Collections.singletonList("查客户"), target.getSkip().get(0).getSourceApis());

		assertEquals(1, target.getExtra().size());
		assertEquals("_id_", target.getExtra().get(0).getName());
		assertTrue(target.getExtra().get(0).getSourceApis().isEmpty(), "目标多出的不是任何 API 声明的");
	}

	@Test
	@DisplayName("同一条索引被多个 API 声明 → 来源列出全部")
	void sourceApisListEveryDeclarer() {
		ServingIndexLandingReport report = reportOf(
				Arrays.asList(
						api("查客户", "CUSTOMER", idx("a_1", null, f("a", true))),
						api("查客户明细", "CUSTOMER", idx("异名同字段", null, f("a", true)))),
				Collections.emptyList());

		assertEquals(Arrays.asList("查客户", "查客户明细"),
				report.getTargets().get(0).getCreate().get(0).getSourceApis());
	}

	@Test
	@DisplayName("唯一索引高亮；unique 不一致只在跳过桶记一笔注记，不改变动作")
	void uniqueHighlightAndMismatchNote() {
		ServingIndexLandingReport report = reportOf(
				Collections.singletonList(api("查客户", "CUSTOMER",
						idx("a_1", Boolean.TRUE, f("a", true)), idx("b_-1", Boolean.TRUE, f("b", false)))),
				Collections.singletonList(existing("a_1", Boolean.FALSE, "a", true)));

		ServingIndexTargetReport target = report.getTargets().get(0);
		assertTrue(target.getCreate().get(0).isUnique(), "唯一索引要高亮");
		assertTrue(target.getSkip().get(0).isUniqueMismatch(), "声明 unique、目标非 unique → 注记");
		assertEquals(1, target.getCreate().size(), "注记不改变动作：仍然只建缺的那条");
	}

	@Test
	@DisplayName("索引总数水位从三桶推出：≥50 提示 / ≥60 严重 / ≥64 预检失败")
	void indexCountLevels() {
		assertEquals(ServingIndexTargetReport.IndexCountLevel.OK, levelWith(10, 1));
		assertEquals(ServingIndexTargetReport.IndexCountLevel.APPROACHING, levelWith(49, 1));
		assertEquals(ServingIndexTargetReport.IndexCountLevel.SEVERE, levelWith(59, 1));
		assertEquals(ServingIndexTargetReport.IndexCountLevel.EXCEEDS_LIMIT, levelWith(63, 1));
	}

	/** 目标已有 existingCount 条无关索引，声明 createCount 条新的 → 报告的水位。 */
	private static ServingIndexTargetReport.IndexCountLevel levelWith(int existingCount, int createCount) {
		List<TapIndex> targetIndexes = new ArrayList<>();
		for (int i = 0; i < existingCount; i++) {
			targetIndexes.add(existing("old_" + i, null, "old_" + i, true));
		}
		ServingIndex[] declared = new ServingIndex[createCount];
		for (int i = 0; i < createCount; i++) {
			declared[i] = idx("new_" + i, null, f("new_" + i, true));
		}
		ServingIndexLandingReport report = reportOf(
				Collections.singletonList(api("查客户", "CUSTOMER", declared)), targetIndexes);
		ServingIndexTargetReport target = report.getTargets().get(0);
		assertEquals(existingCount, target.getExistingCount());
		assertEquals(existingCount + createCount, target.getProjectedCount());
		return target.getLevel();
	}

	@Test
	@DisplayName("读回失败的 target：三桶为空但带 error——不能被读成「无事可做」")
	void failedTargetIsNotAnEmptySuccess() {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", "CUSTOMER", idx("a_1", null, f("a", true)))), conMap());
		ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
		outcome.setTableName("CUSTOMER");
		outcome.setError("read back timeout, engine offline or no reply");
		work.getOutcomes().add(outcome);

		ServingIndexLandingReport report = ServingIndexLandingReports.of(work);
		ServingIndexTargetReport target = report.getTargets().get(0);
		assertTrue(target.getCreate().isEmpty());
		assertEquals("read back timeout, engine offline or no reply", target.getError());
		assertTrue(report.needsAttention());
	}

	@Test
	@DisplayName("落不了地的声明进报告，不静默丢弃；一切正常时 needsAttention 为假")
	void unresolvedSurfacesAndCleanRunNeedsNoAttention() {
		ServingIndexLandingReport withGap = ServingIndexLandingReports.of(
				ServingIndexLandingTargets.from(
						Collections.singletonList(api("查客户", "CUSTOMER", idx("a_1", null, f("a", true)))),
						new LinkedHashMap<>()));
		assertEquals(1, withGap.getUnresolved().size());
		assertTrue(withGap.needsAttention());

		ServingIndexLandingReport clean = reportOf(
				Collections.singletonList(api("查客户", "CUSTOMER", idx("a_1", null, f("a", true)))),
				Collections.singletonList(existing("a_1", null, "a", true)));
		assertFalse(clean.needsAttention());
	}

	@Test
	@DisplayName("P3-5：报告自带汇总——看的人不该自己把三桶加起来数失败")
	void reportCarriesTheSummary() {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(
				Collections.singletonList(api("查客户", "CUSTOMER", idx("a_1", null, f("a", true)))), conMap());
		ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
		outcome.setConnectionName("MDM-生产库");
		outcome.setTableName("CUSTOMER");
		outcome.setError("read back timeout, engine offline or no reply");
		work.getOutcomes().add(outcome);

		ServingIndexLandingSummary summary = ServingIndexLandingReports.of(work).getSummary();

		assertEquals(1, summary.getTargets());
		assertEquals(1, summary.getFailed());
		assertFalse(summary.isClean());
	}
}
