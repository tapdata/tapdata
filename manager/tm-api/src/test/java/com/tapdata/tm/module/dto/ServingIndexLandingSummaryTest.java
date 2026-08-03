package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-5 · 一次落地的汇总（{@link ServingIndexLandingSummary}）。TAP-12057（方案 §4 风险表）。
 *
 * <p>「不首错即停」的另一半是<b>收集汇总</b>：逐 {@code (连接,集合)} 各走各的之后，得有一处把
 * 「谁建了 / 谁没建 / 谁根本落不了地」一次说清——否则失败散落在几十行日志里，等于没报。</p>
 *
 * <p>两条口径钉在这里：① conMap 未命中<b>算问题</b>（哪怕每个 target 都成功，本次落地也不干净——
 * 那正是「部署报成功、索引没建」的老毛病）；② 连接器不支持建索引<b>不算部署失败</b>（方案 §4
 * 「非 MongoDB 目标」记 skipped-unsupported）。</p>
 */
class ServingIndexLandingSummaryTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	private static ServingIndexTargetOutcome outcome(String table, String error, String... failed) {
		ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
		outcome.setConnectionId(CONNECTION_ID);
		outcome.setConnectionName("MDM-生产库");
		outcome.setTableName(table);
		outcome.setError(error);
		outcome.getFailed().addAll(Arrays.asList(failed));
		if (error == null) {
			outcome.setPlan(new ServingIndexLandingPlan());
		}
		return outcome;
	}

	private static ServingIndexLandingWorkList work(ServingIndexTargetOutcome... outcomes) {
		ServingIndexLandingWorkList work = new ServingIndexLandingWorkList();
		for (ServingIndexTargetOutcome outcome : outcomes) {
			work.getOutcomes().add(outcome);
			work.getTargets().add(new ServingIndexLandingTarget(null, outcome.getTableName()));
		}
		return work;
	}

	@Test
	@DisplayName("全部走完 → clean，建成条数汇总出来")
	void countsCreatedAcrossTargets() {
		ServingIndexTargetOutcome first = outcome("CUSTOMER", null);
		first.getCreated().addAll(Arrays.asList("a_1", "b_-1"));
		ServingIndexTargetOutcome second = outcome("ORDER", null);
		second.getCreated().add("c_1");

		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(work(first, second));

		assertTrue(summary.isClean());
		assertEquals(2, summary.getTargets());
		assertEquals(2, summary.getSucceeded());
		assertEquals(0, summary.getFailed());
		assertEquals(3, summary.getCreated());
		assertTrue(summary.getProblems().isEmpty());
	}

	@Test
	@DisplayName("一个 target 塌了 → 不 clean，且问题行点名 (连接,集合)")
	void failedTargetIsNamedWithItsConnectionAndTable() {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(
				work(outcome("CUSTOMER", "read back timeout, engine offline or no reply"),
						outcome("ORDER", null)));

		assertFalse(summary.isClean());
		assertEquals(1, summary.getFailed());
		assertEquals(1, summary.getSucceeded());
		assertEquals(1, summary.getProblems().size());
		String problem = summary.getProblems().get(0);
		assertTrue(problem.contains("CUSTOMER"), problem);
		assertTrue(problem.contains("MDM-生产库"), problem);
		assertTrue(problem.contains("timeout"), problem);
	}

	@Test
	@DisplayName("conMap 未命中即使 target 全成功也不 clean——静默跳过等于「部署报成功、索引没建」")
	void unresolvedDeclarationsAreAlwaysAProblem() {
		ServingIndexLandingWorkList work = work(outcome("ORDER", null));
		work.getUnresolved().add(new UnresolvedServingIndexTarget("查客户", "m1", CONNECTION_ID, "CUSTOMER", 2,
				UnresolvedServingIndexTarget.Reason.CONNECTION_UNRESOLVED));

		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(work);

		assertFalse(summary.isClean());
		assertEquals(1, summary.getUnresolved());
		assertEquals(0, summary.getFailed());
		String problem = summary.getProblems().get(0);
		assertTrue(problem.contains("查客户"), problem);
		assertTrue(problem.contains("CONNECTION_UNRESOLVED"), problem);
	}

	@Test
	@DisplayName("权限不足被点名，而不是埋在驱动原文里")
	void permissionDeniedIsCalledOut() {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(work(outcome("CUSTOMER", null,
				"a_1: Command failed with error 13 (Unauthorized): 'not authorized on mdm to execute command "
						+ "{ createIndexes: \"CUSTOMER\" }'")));

		assertTrue(summary.isPermissionDenied());
		assertFalse(summary.isClean());
		assertEquals(1, summary.getIndexFailures());
		assertTrue(summary.describe().toLowerCase().contains("privilege"), summary.describe());
	}

	@Test
	@DisplayName("连接器不支持建索引：计数、但不算部署失败（方案 §4 非 MongoDB 目标）")
	void unsupportedConnectorIsNotADeployFailure() {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(work(
				outcome("CUSTOMER", null, "a_1: Connector does not support create index")));

		assertEquals(1, summary.getUnsupported());
		assertEquals(0, summary.getIndexFailures());
		assertEquals(0, summary.getFailed());
		assertTrue(summary.isClean(), "不支持不是失败——否则非 Mongo 目标的部署会被判成红");
	}

	@Test
	@DisplayName("逐条失败里混着不支持与真失败 → 真失败仍让 target 判失败")
	void realFailureStillCountsWhenMixedWithUnsupported() {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(work(outcome("CUSTOMER", null,
				"a_1: Connector does not support create index",
				"b_-1: E11000 duplicate key error collection: mdm.CUSTOMER index: b_-1")));

		assertEquals(1, summary.getUnsupported());
		assertEquals(1, summary.getIndexFailures());
		assertEquals(1, summary.getFailed());
		assertFalse(summary.isClean());
	}

	@Test
	@DisplayName("没活可干 → clean，describe 也说得出口")
	void emptyWorkListIsClean() {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(new ServingIndexLandingWorkList());

		assertTrue(summary.isClean());
		assertEquals(0, summary.getTargets());
		assertFalse(summary.describe().isEmpty());
	}

	@Test
	@DisplayName("null 工作表不炸（调用点在导入主链路上，绝不能因为汇总把导入带崩）")
	void toleratesNullWorkList() {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(null);

		assertTrue(summary.isClean());
		assertEquals(0, summary.getTargets());
		assertEquals(Collections.emptyList(), summary.getProblems());
	}
}
