package com.tapdata.tm.module.dto;

import com.tapdata.tm.base.dto.ResponseMessage;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 把落地报告压成 CICD 部署计划表（纯函数）。TAP-12057 · P4-1（方案 §3.5 / <b>ADR-0005</b> / <b>ADR-0013</b>）。
 *
 * <p>索引腿要在部署计划表里<b>显式可见、可单独审阅</b>，而 worker 侧的 preview/import 是所有资源类型
 * 共用的通用脚本（只认 {@code add/update/delete} 与 {@code code != "ok"}）。形状对齐因此放在 TM 这边，
 * P4-2 只加一条 URL 分支。</p>
 *
 * <p><b>preview 与 import 的 {@code add} 口径不同，这是刻意的</b>：preview 列「将创建」（计划），
 * import 列「真建成」（事实——回执说建了但复核发现库里没有的那条已被落地服务
 * {@code ServingIndexLandingService} 从 {@code created} 里剔走，ADR-0013）。拿计划桶充当结果，
 * 等于把连接器吞掉 85/86 的那句
 * 「成功」原样转述给 CICD，正是本工单要消灭的谎。</p>
 */
public final class ServingIndexPlanDiffs {

	/** 索引腿有问题时回给 worker 的 code——非 {@code ok} 即让这一步红（脚本只认这个）。 */
	public static final String ERROR_CODE = "ServingIndex.Landing.HasProblems";

	private ServingIndexPlanDiffs() {
	}

	/** dry-run：把「将创建」桶铺成计划表，富报告原样带上。不带数据源类型 = 不出手工语句。 */
	public static ServingIndexPreviewResult preview(ServingIndexLandingReport report) {
		return preview(report, java.util.Collections.emptyMap());
	}

	/**
	 * 同上，并按 {@code 目标连接 id → database_type} 为 MongoDB 目标附手工执行语句。
	 */
	public static ServingIndexPreviewResult preview(ServingIndexLandingReport report,
													java.util.Map<String, String> databaseTypeByConnectionId) {
		ServingIndexPreviewResult result = new ServingIndexPreviewResult();
		result.setReport(report);
		if (report == null) {
			return result;
		}
		for (ServingIndexTargetReport target : report.getTargets()) {
			String databaseType = databaseTypeByConnectionId == null
					? null
					: databaseTypeByConnectionId.get(target.getConnectionId());
			for (ServingIndexReportEntry entry : target.getCreate()) {
				result.getAdd().add(row(target.getConnectionName(), target.getTableName(), entry.getName(),
						entry.getFields(), entry.isUnique(), entry.getSourceApis()));
				// 手工执行语句只对 MongoDB 生成；语句不进表格（太长会把计划表挤垮），
				// 与 add 平行放一份，worker 渲染成表格下方的代码块。
				String command = ServingIndexManualCommands.of(databaseType, target.getTableName(),
						new ServingIndex(entry.getName(), entry.isUnique(), entry.getFields()));
				if (command != null) {
					result.getCommands().add(command);
				}
			}
		}
		return result;
	}

	/** 真跑：只列真建成的那些（见类注释），报告则是逐 target 结果全文。 */
	public static ServingIndexImportResult imported(ServingIndexLandingWorkList work) {
		ServingIndexImportResult result = new ServingIndexImportResult();
		result.setReport(ServingIndexLandingReports.of(work));
		if (work == null) {
			return result;
		}
		for (int i = 0; i < work.getOutcomes().size(); i++) {
			ServingIndexTargetOutcome outcome = work.getOutcomes().get(i);
			ServingIndexLandingTarget target = i < work.getTargets().size() ? work.getTargets().get(i) : null;
			if (outcome.getPlan() == null) {
				// 读回就失败，一条都没建；失败本身走 problemOf，不在计划表里冒充变更。
				continue;
			}
			for (ServingIndex creation : outcome.getPlan().getCreate()) {
				if (!outcome.getCreated().contains(creation.getName())) {
					continue;
				}
				result.getDiff().getAdd().add(row(outcome.getConnectionName(), outcome.getTableName(),
						creation.getName(), creation.getFields(), Boolean.TRUE.equals(creation.getUnique()),
						apisOf(creation, target)));
			}
		}
		return result;
	}

	/**
	 * 这次索引腿有没有「该让人管一管」的事——有则返回一句话说清是什么，无则 {@code null}。
	 *
	 * <p>三类都要红：① 汇总里的逐项失败与落不了地（P3-5 已按 {@code (连接,集合)} 说明白）；
	 * ② 报告里带 error 的 target——读回失败时三桶为空，<b>空计划表最容易被读成「无事可做」</b>；
	 * ③ 索引总数越过 MongoDB 单集合 64 上限（ADR-0008 的预检）。</p>
	 *
	 * <p>索引腿幂等，重跑的代价只是再读一次；所以宁可红一次让人看一眼，也不静默放行。</p>
	 */
	public static String problemOf(ServingIndexLandingReport report) {
		if (report == null) {
			return null;
		}
		Set<String> problems = new LinkedHashSet<>();
		if (report.getSummary() != null) {
			problems.addAll(report.getSummary().getProblems());
		}
		for (ServingIndexTargetReport target : report.getTargets()) {
			// 汇总已按 `连接(id).表: ` 开头点过名的就不再重复一遍。
			if (target.getError() != null && !mentioned(problems, target)) {
				problems.add(where(target) + ": " + target.getError());
			}
			if (target.getLevel() == ServingIndexTargetReport.IndexCountLevel.EXCEEDS_LIMIT) {
				problems.add(where(target) + ": index count would reach " + target.getProjectedCount()
						+ ", over MongoDB's per-collection limit of " + ServingIndexTargetReport.HARD_LIMIT);
			}
		}
		if (problems.isEmpty() && !report.getUnresolved().isEmpty()) {
			// 正常路径下 unresolved 已进汇总；这里兜住「报告没带汇总」的调用方，别让它悄悄放行。
			for (UnresolvedServingIndexTarget gap : report.getUnresolved()) {
				problems.add(gap.getApiName() + ": " + gap.getReason());
			}
		}
		return problems.isEmpty() ? null : String.join("; ", problems);
	}

	/**
	 * 把问题翻成 worker 认得的失败信号：{@code code} 置为 {@link #ERROR_CODE}、{@code message} 带上问题，
	 * <b>{@code data} 原样留着</b>——脚本会把整个响应体打进 CI 日志，报告不能因为这次失败就丢。
	 */
	public static void applyProblem(ResponseMessage<?> response, String problem) {
		if (response == null || problem == null) {
			return;
		}
		response.setCode(ERROR_CODE);
		response.setMessage("serving index landing has problems: " + problem);
	}

	private static ServingIndexPlanRow row(String connection, String table, String name,
										   List<ServingIndexField> fields, boolean unique, List<String> sourceApis) {
		ServingIndexPlanRow row = new ServingIndexPlanRow();
		row.setConnection(connection);
		row.setTable(table);
		row.setName(name);
		// 展示串 = 身份签名，见 ServingIndexSignature#ofFields。
		row.setKeys(ServingIndexSignature.ofFields(fields));
		row.setUnique(unique);
		row.setDeclaredBy(sourceApis == null ? "" : String.join(", ", sourceApis));
		return row;
	}

	private static List<String> apisOf(ServingIndex index, ServingIndexLandingTarget target) {
		if (target == null) {
			return new ArrayList<>();
		}
		List<String> apis = target.getDeclaredBy().get(ServingIndexSignature.of(index));
		return apis == null ? new ArrayList<>() : new ArrayList<>(apis);
	}

	private static boolean mentioned(Set<String> problems, ServingIndexTargetReport target) {
		String prefix = where(target) + ":";
		for (String problem : problems) {
			if (problem.startsWith(prefix)) {
				return true;
			}
		}
		return false;
	}

	/** 与 P3-5 汇总同一种点名方式：`连接名(连接id).表`——现场靠它定位是哪个库的哪张表。 */
	private static String where(ServingIndexTargetReport target) {
		return target.getConnectionName() + "(" + target.getConnectionId() + ")." + target.getTableName();
	}
}
