package com.tapdata.tm.module.dto;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P4-1 · 把落地报告压成 CICD 计划表（纯函数）。TAP-12057（方案 §3.5）。
 *
 * <p>worker 的 {@code preview-resource.sh} / {@code import-resource.sh} 是<b>通用脚本</b>：
 * 前者只认 {@code .data.add/.update/.delete} 并据此渲染部署计划表，后者只认 {@code .data.diff}
 * 且仅在 {@code code != "ok"} 时让这一步红。P4-2 因此只需加一条 URL 分支——形状得由 TM 这边对齐。</p>
 *
 * <p>两条最要紧的口径钉在这里：① <b>{@code update}/{@code delete} 恒空</b>（只加不删，ADR-0005/0008），
 * ② <b>真跑的 {@code add} 只列真建成的那些</b>——按计划桶列会把「回执说建了、复核发现没建成」的那条
 * 也写进 {@code changed_indexes}，那正是本工单要消灭的谎（ADR-0013）。</p>
 */
class ServingIndexPlanDiffsTest {

	private static ServingIndexField field(String name, boolean asc) {
		return new ServingIndexField(name, asc);
	}

	private static ServingIndex index(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	private static ServingIndexReportEntry entry(String name, boolean unique, List<String> apis,
												 ServingIndexField... fields) {
		ServingIndexReportEntry entry = new ServingIndexReportEntry();
		entry.setName(name);
		entry.getFields().addAll(Arrays.asList(fields));
		entry.setUnique(unique);
		entry.setSourceApis(new ArrayList<>(apis));
		return entry;
	}

	private static ServingIndexTargetReport target(String connection, String table) {
		ServingIndexTargetReport target = new ServingIndexTargetReport();
		target.setConnectionId(new ObjectId().toHexString());
		target.setConnectionName(connection);
		target.setTableName(table);
		return target;
	}

	@Nested
	@DisplayName("preview：把「将创建」桶铺成计划表")
	class Preview {

		@Test
		@DisplayName("每条将创建的索引一行，带连接 / 集合 / 索引名 / 键与方向 / unique / 来源 API")
		void oneRowPerPlannedIndex() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexTargetReport target = target("fdm", "MDM_CUSTOMER");
			target.getCreate().add(entry("CUSTOMER_ID_1", false, Collections.singletonList("customer"),
					field("CUSTOMER_ID", true)));
			report.getTargets().add(target);

			ServingIndexPreviewResult result = ServingIndexPlanDiffs.preview(report);

			assertEquals(1, result.getAdd().size());
			ServingIndexPlanRow row = result.getAdd().get(0);
			assertEquals("fdm", row.getConnection());
			assertEquals("MDM_CUSTOMER", row.getTable());
			assertEquals("CUSTOMER_ID_1", row.getName());
			assertEquals("CUSTOMER_ID:1", row.getKeys());
			assertFalse(row.isUnique());
			assertEquals("customer", row.getDeclaredBy());
			assertSame(report, result.getReport(), "富报告要原样带上，计划表只是它的摘要");
		}

		@Test
		@DisplayName("降序渲染成 -1，复合索引按声明顺序拼——方向看不见就等于没修 P0")
		void rendersDirectionAndCompositeOrder() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexTargetReport target = target("fdm", "MDM_CUSTOMER");
			target.getCreate().add(entry("CUSTOMER_ID_-1_CITY_1", true, Collections.singletonList("customer"),
					field("CUSTOMER_ID", false), field("CITY", true)));
			report.getTargets().add(target);

			ServingIndexPlanRow row = ServingIndexPlanDiffs.preview(report).getAdd().get(0);

			assertEquals("CUSTOMER_ID:-1,CITY:1", row.getKeys());
			assertEquals(ServingIndexSignature.of(index("x", false, field("CUSTOMER_ID", false), field("CITY", true))),
					row.getKeys(), "计划表上的键串必须就是身份签名——展示与比对各写一份迟早对不上（§2.4 的教训）");
			assertTrue(row.isUnique());
		}

		@Test
		@DisplayName("多个 target 的将创建桶合并成一张表；同一条索引的多个来源 API 逗号相连")
		void mergesTargetsAndJoinsSourceApis() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexTargetReport one = target("fdm", "MDM_CUSTOMER");
			one.getCreate().add(entry("CUSTOMER_ID_1", false, Arrays.asList("customer", "customer v2"),
					field("CUSTOMER_ID", true)));
			ServingIndexTargetReport two = target("mdm", "MDM_POLICY");
			two.getCreate().add(entry("POLICY_ID_1", false, Collections.singletonList("policy"),
					field("POLICY_ID", true)));
			report.getTargets().add(one);
			report.getTargets().add(two);

			List<ServingIndexPlanRow> add = ServingIndexPlanDiffs.preview(report).getAdd();

			assertEquals(2, add.size());
			assertEquals("customer, customer v2", add.get(0).getDeclaredBy());
			assertEquals("MDM_POLICY", add.get(1).getTable());
		}

		@Test
		@DisplayName("将跳过 / 目标多出都不进计划表——部署这一步不动它们")
		void skipAndExtraNeverEnterThePlan() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexTargetReport target = target("fdm", "MDM_CUSTOMER");
			target.getSkip().add(entry("CUSTOMER_ID_1", false, Collections.singletonList("customer"),
					field("CUSTOMER_ID", true)));
			target.getExtra().add(entry("LEGACY_1", false, Collections.emptyList(), field("LEGACY", true)));
			report.getTargets().add(target);

			ServingIndexPreviewResult result = ServingIndexPlanDiffs.preview(report);

			assertTrue(result.getAdd().isEmpty());
			assertTrue(result.getUpdate().isEmpty());
			assertTrue(result.getDelete().isEmpty());
		}

		@Test
		@DisplayName("update / delete 恒空：索引身份没有「改」，多出的也绝不删（ADR-0005 / ADR-0008）")
		void updateAndDeleteAreAlwaysEmpty() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexTargetReport target = target("fdm", "MDM_CUSTOMER");
			target.getCreate().add(entry("A_1", false, Collections.emptyList(), field("A", true)));
			target.getExtra().add(entry("B_1", false, Collections.emptyList(), field("B", true)));
			report.getTargets().add(target);

			ServingIndexPreviewResult result = ServingIndexPlanDiffs.preview(report);

			assertNotNull(result.getUpdate());
			assertNotNull(result.getDelete());
			assertTrue(result.getUpdate().isEmpty());
			assertTrue(result.getDelete().isEmpty());
		}

		@Test
		@DisplayName("空报告 / null 报告都给出空计划表，不炸")
		void toleratesEmptyReport() {
			assertTrue(ServingIndexPlanDiffs.preview(new ServingIndexLandingReport()).getAdd().isEmpty());
			assertTrue(ServingIndexPlanDiffs.preview(null).getAdd().isEmpty());
		}
	}

	@Nested
	@DisplayName("imported：真跑之后，只列真建成的")
	class Imported {

		private ServingIndexLandingWorkList work(List<String> created, ServingIndex... planned) {
			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(new ObjectId());
			connection.setName("fdm");
			ServingIndexLandingTarget target = new ServingIndexLandingTarget(connection, "MDM_CUSTOMER");
			ServingIndexLandingPlan plan = new ServingIndexLandingPlan();
			for (ServingIndex index : planned) {
				plan.getCreate().add(index);
				target.getDeclared().add(index);
			}
			ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
			outcome.setConnectionId(connection.getId().toHexString());
			outcome.setConnectionName("fdm");
			outcome.setTableName("MDM_CUSTOMER");
			outcome.setPlan(plan);
			outcome.getCreated().addAll(created);

			ServingIndexLandingWorkList work = new ServingIndexLandingWorkList();
			work.getTargets().add(target);
			work.getOutcomes().add(outcome);
			return work;
		}

		@Test
		@DisplayName("回执认了、复核也认了的那条进 add")
		void createdIndexesAreReported() {
			ServingIndexLandingWorkList work = work(Collections.singletonList("CUSTOMER_ID_1"),
					index("CUSTOMER_ID_1", false, field("CUSTOMER_ID", true)));

			ServingIndexImportResult result = ServingIndexPlanDiffs.imported(work);

			assertEquals(1, result.getDiff().getAdd().size());
			assertEquals("CUSTOMER_ID_1", result.getDiff().getAdd().get(0).getName());
			assertEquals("CUSTOMER_ID:1", result.getDiff().getAdd().get(0).getKeys());
			assertNotNull(result.getReport(), "真跑也要带上富报告");
		}

		@Test
		@DisplayName("计划里有、却没建成的，绝不进 add——那是谎（ADR-0013 建后复核就是为它而设）")
		void plannedButNotCreatedIsNeverReportedAsAdded() {
			ServingIndexLandingWorkList work = work(Collections.emptyList(),
					index("CUSTOMER_ID_-1", false, field("CUSTOMER_ID", false)));

			ServingIndexImportResult result = ServingIndexPlanDiffs.imported(work);

			assertTrue(result.getDiff().getAdd().isEmpty(),
					"连接器吞掉 85/86 报成功的那条，复核已把它从 created 里剔走，计划表也不许再声称建了");
		}

		@Test
		@DisplayName("同一 target 里建成与没建成混着时，只挑建成的")
		void picksOnlyTheCreatedOnesWhenMixed() {
			ServingIndexLandingWorkList work = work(Collections.singletonList("A_1"),
					index("A_1", false, field("A", true)), index("B_-1", false, field("B", false)));

			List<ServingIndexPlanRow> add = ServingIndexPlanDiffs.imported(work).getDiff().getAdd();

			assertEquals(1, add.size());
			assertEquals("A_1", add.get(0).getName());
		}

		@Test
		@DisplayName("null 工作表给空结果，不炸")
		void toleratesNullWorkList() {
			ServingIndexImportResult result = ServingIndexPlanDiffs.imported(null);
			assertTrue(result.getDiff().getAdd().isEmpty());
		}
	}

	@Nested
	@DisplayName("problemOf：什么情况该让 CICD 这一步红")
	class Problems {

		private ServingIndexLandingReport reportWithProblems(String... problems) {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexLandingSummary summary = new ServingIndexLandingSummary();
			summary.getProblems().addAll(Arrays.asList(problems));
			report.setSummary(summary);
			return report;
		}

		@Test
		@DisplayName("干净就没有问题串——放行")
		void cleanReportHasNoProblem() {
			assertNull(ServingIndexPlanDiffs.problemOf(new ServingIndexLandingReport()));
			assertNull(ServingIndexPlanDiffs.problemOf(null));
		}

		@Test
		@DisplayName("逐项失败要让这一步红，且消息里点名具体是哪条")
		void indexFailureIsReportedWithDetail() {
			String problem = ServingIndexPlanDiffs.problemOf(
					reportWithProblems("fdm.MDM_CUSTOMER: CUSTOMER_ID_-1: reported created but absent on re-read"));

			assertNotNull(problem);
			assertTrue(problem.contains("CUSTOMER_ID_-1"),
					"CI 现场只看得见这一行，不点名等于没报（P3-5 汇总口径）");
		}

		@Test
		@DisplayName("有声明落不了地（conMap 未命中）也要红——静默跳过正是 §2.3 那个既有缺陷")
		void unresolvedTargetIsReported() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			report.getUnresolved().add(new UnresolvedServingIndexTarget("customer", new ObjectId().toHexString(),
					new ObjectId().toHexString(), "MDM_CUSTOMER", 1,
					UnresolvedServingIndexTarget.Reason.CONNECTION_UNRESOLVED));

			assertNotNull(ServingIndexPlanDiffs.problemOf(report));
		}

		@Test
		@DisplayName("读回失败导致计划为空时也要红——空计划表会被人读成「无事可做」")
		void unusableReadbackIsReported() {
			ServingIndexLandingReport report = new ServingIndexLandingReport();
			ServingIndexTargetReport target = target("fdm", "MDM_CUSTOMER");
			target.setError("read back timeout, engine offline or no reply");
			report.getTargets().add(target);

			assertNotNull(ServingIndexPlanDiffs.problemOf(report));
		}
	}

	@Nested
	@DisplayName("applyProblem：把问题翻成 worker 认得的失败信号")
	class ApplyProblem {

		@Test
		@DisplayName("有问题时 code 不再是 ok，message 带上问题，data 仍然完整——报告不能因为失败就丢")
		void flagsFailureButKeepsPayload() {
			ResponseMessage<String> response = new ResponseMessage<>();
			response.setData("payload");

			ServingIndexPlanDiffs.applyProblem(response, "boom");

			assertEquals(ServingIndexPlanDiffs.ERROR_CODE, response.getCode());
			assertTrue(response.getMessage().contains("boom"));
			assertEquals("payload", response.getData(), "worker 会把整个 body 打进 CI 日志，报告得留着");
		}

		@Test
		@DisplayName("没问题就一个字都不改")
		void leavesCleanResponseAlone() {
			ResponseMessage<String> response = new ResponseMessage<>();
			response.setData("payload");

			ServingIndexPlanDiffs.applyProblem(response, null);

			assertEquals(ResponseMessage.OK, response.getCode());
			assertNull(response.getMessage());
		}
	}
}
