package com.tapdata.tm.module.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 由落地工作表产出 dry-run 分桶报告（纯函数）。TAP-12057 · P3-4（方案 §3.4 / <b>ADR-0005</b> / <b>ADR-0008</b>）。
 *
 * <p>报告是<b>给人看的决策依据</b>，不是执行记录：三桶（将创建 / 将跳过 / 目标多出）+ 每条的来源 API +
 * 唯一索引高亮 + 索引总数水位。「目标多出」<b>只列出、绝不删</b>（只加不删）。</p>
 *
 * <p><b>来源 API 按签名回查</b>（P3-2 定的口径）：归属留在 {@link ServingIndexLandingTarget#getDeclaredBy()}，
 * 不塞进每条 {@link ServingIndex}——否则同一条索引被多个 API 声明时就得在计划里存重复副本。</p>
 *
 * <p><b>索引总数水位从三桶推出，不需要额外读库</b>：目标现有索引要么被声明命中（skip）、要么多出（extra），
 * 两者之和即现有数；加上将创建即落地后的数。≥50 提示、≥60 严重、≥64 预检失败（Mongo 单集合硬上限）。</p>
 */
public final class ServingIndexLandingReports {

	private ServingIndexLandingReports() {
	}

	/** 把整张工作表（含逐 target 结果）转成报告；unresolved 原样带过去，报告要显示「有声明却落不了地」。 */
	public static ServingIndexLandingReport of(ServingIndexLandingWorkList work) {
		ServingIndexLandingReport report = new ServingIndexLandingReport();
		if (work == null) {
			return report;
		}
		report.getUnresolved().addAll(work.getUnresolved());
		report.setSummary(ServingIndexLandingSummary.of(work));
		for (int i = 0; i < work.getOutcomes().size(); i++) {
			ServingIndexTargetOutcome outcome = work.getOutcomes().get(i);
			ServingIndexLandingTarget target = i < work.getTargets().size() ? work.getTargets().get(i) : null;
			report.getTargets().add(of(outcome, target));
		}
		return report;
	}

	static ServingIndexTargetReport of(ServingIndexTargetOutcome outcome, ServingIndexLandingTarget target) {
		ServingIndexTargetReport targetReport = new ServingIndexTargetReport();
		targetReport.setConnectionId(outcome.getConnectionId());
		targetReport.setConnectionName(outcome.getConnectionName());
		targetReport.setTableName(outcome.getTableName());
		targetReport.setError(outcome.getError());

		ServingIndexLandingPlan plan = outcome.getPlan();
		if (plan == null) {
			// 读回就失败：三桶为空但这不是「无事可做」，靠 error 区分（调用方别把空报告读成幂等成功）。
			return targetReport;
		}
		Map<String, List<String>> declaredBy = target == null ? Collections.emptyMap() : target.getDeclaredBy();

		for (ServingIndex creation : plan.getCreate()) {
			targetReport.getCreate().add(entry(creation, false, declaredBy));
		}
		for (SkippedServingIndex skipped : plan.getSkip()) {
			// 展示用目标环境实际那条（名字是目标的名），但归属与 unique 注记来自声明侧。
			ServingIndexReportEntry entry = entry(skipped.getExisting(), skipped.isUniqueMismatch(), declaredBy);
			entry.setSourceApis(apisOf(skipped.getDeclared(), declaredBy));
			targetReport.getSkip().add(entry);
		}
		for (ServingIndex spare : plan.getExtra()) {
			// 目标多出：不是任何 API 声明的，来源留空。
			targetReport.getExtra().add(entry(spare, false, Collections.emptyMap()));
		}

		int existing = plan.getSkip().size() + plan.getExtra().size();
		targetReport.setExistingCount(existing);
		targetReport.setProjectedCount(existing + plan.getCreate().size());
		targetReport.setLevel(levelOf(targetReport.getProjectedCount()));
		return targetReport;
	}

	private static ServingIndexReportEntry entry(ServingIndex index, boolean uniqueMismatch,
												 Map<String, List<String>> declaredBy) {
		ServingIndexReportEntry entry = new ServingIndexReportEntry();
		entry.setName(index.getName());
		if (index.getFields() != null) {
			for (ServingIndexField field : index.getFields()) {
				entry.getFields().add(new ServingIndexField(field.getField(), field.getAsc()));
			}
		}
		entry.setUnique(Boolean.TRUE.equals(index.getUnique()));
		entry.setUniqueMismatch(uniqueMismatch);
		entry.setSourceApis(apisOf(index, declaredBy));
		return entry;
	}

	private static List<String> apisOf(ServingIndex index, Map<String, List<String>> declaredBy) {
		List<String> apis = declaredBy.get(ServingIndexSignature.of(index));
		return apis == null ? new ArrayList<>() : new ArrayList<>(apis);
	}

	private static ServingIndexTargetReport.IndexCountLevel levelOf(int projected) {
		if (projected >= ServingIndexTargetReport.HARD_LIMIT) {
			return ServingIndexTargetReport.IndexCountLevel.EXCEEDS_LIMIT;
		}
		if (projected >= ServingIndexTargetReport.SEVERE_AT) {
			return ServingIndexTargetReport.IndexCountLevel.SEVERE;
		}
		if (projected >= ServingIndexTargetReport.WARN_AT) {
			return ServingIndexTargetReport.IndexCountLevel.APPROACHING;
		}
		return ServingIndexTargetReport.IndexCountLevel.OK;
	}
}
