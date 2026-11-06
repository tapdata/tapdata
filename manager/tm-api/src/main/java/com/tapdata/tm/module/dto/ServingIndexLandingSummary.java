package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一次落地的汇总（纯函数产物）。TAP-12057 · P3-5（方案 §4 风险表）。
 *
 * <p>「不首错即停」的另一半是<b>收集汇总</b>：逐 {@code (连接,集合)} 各走各的之后，得有一处把
 * 「建了什么 / 谁没建成 / 谁根本落不了地」一次说清——失败散在几十行日志里等于没报，
 * 而「部署报成功、索引没建」正是本工单要消灭的那种谎。</p>
 *
 * <p><b>两条口径</b>：</p>
 * <ul>
 *   <li><b>conMap 未命中算问题</b>：哪怕每个 target 都成功，只要有声明落不了地，本次落地就不 clean。
 *       恢复办法是改连接名后重跑 indexes 腿（幂等，已建项 skip）。</li>
 *   <li><b>连接器不支持建索引不算部署失败</b>（方案 §4「非 MongoDB 目标」记 skipped-unsupported）：
 *       单独计数、照样可见，但不把非 Mongo 目标的部署判成红。<b>故本类的「失败」口径与
 *       {@link ServingIndexTargetOutcome#isSucceeded()} 刻意不同</b>——那边是「这一腿有没有原样走完」，
 *       这边是「这次部署要不要人来管」。</li>
 * </ul>
 *
 * <p>纯函数、不触库不触 Spring：可离线 TDD（{@code ServingIndexLandingSummaryTest}）。</p>
 */
@Data
public class ServingIndexLandingSummary implements Serializable {

	private static final long serialVersionUID = 1L;

	/** {@code (连接,集合)} 工作项总数。 */
	private int targets;

	/** 走完且无真失败的工作项数（只含不支持的算走完）。 */
	private int succeeded;

	/** 有真失败的工作项数（读回/建索引整体失败，或有非「不支持」的逐条失败）。 */
	private int failed;

	/** 有声明却落不了地的记录数（conMap 未命中 / 无表名）。 */
	private int unresolved;

	/** 实际建成的索引条数。 */
	private int created;

	/** 逐条真失败数（不含「连接器不支持」）。 */
	private int indexFailures;

	/** 逐条「连接器不支持建索引」数——可见但不算失败。 */
	private int unsupported;

	/** 是否出现过权限不足：这一条最值得单独喊，人去授权即可恢复、重跑幂等。 */
	private boolean permissionDenied;

	/** 需要人处理的问题，逐 {@code (连接,集合)} / 逐落不了地的声明一行。 */
	private List<String> problems = new ArrayList<>();

	/**
	 * @param work 落地工作表（含逐 target 结果）；{@code null} 返回空汇总——汇总在导入主链路上，绝不能把导入带崩
	 */
	public static ServingIndexLandingSummary of(ServingIndexLandingWorkList work) {
		ServingIndexLandingSummary summary = new ServingIndexLandingSummary();
		if (work == null) {
			return summary;
		}
		summary.targets = work.getTargets().size();
		for (ServingIndexTargetOutcome outcome : work.getOutcomes()) {
			summary.accept(outcome);
		}
		for (UnresolvedServingIndexTarget gap : work.getUnresolved()) {
			summary.unresolved++;
			summary.problems.add(describe(gap));
		}
		return summary;
	}

	private void accept(ServingIndexTargetOutcome outcome) {
		created += outcome.getCreated().size();
		boolean realFailure = outcome.getError() != null;
		if (realFailure && ServingIndexFailures.classify(outcome.getError()) == ServingIndexFailures.Kind.PERMISSION_DENIED) {
			permissionDenied = true;
		}
		List<String> explained = new ArrayList<>();
		for (String failure : outcome.getFailed()) {
			ServingIndexFailures.Kind kind = ServingIndexFailures.classify(failure);
			if (kind == ServingIndexFailures.Kind.UNSUPPORTED) {
				unsupported++;
				continue;
			}
			if (kind == ServingIndexFailures.Kind.PERMISSION_DENIED) {
				permissionDenied = true;
			}
			indexFailures++;
			realFailure = true;
			explained.add(ServingIndexFailures.explain(failure));
		}
		if (!realFailure) {
			succeeded++;
			return;
		}
		failed++;
		problems.add(describe(outcome, explained));
	}

	/** 问题行必须点名 {@code (连接, 集合)}——方案 §4 要求「报告逐 (连接,集合) 列明已建/未建/未命中」。 */
	private static String describe(ServingIndexTargetOutcome outcome, List<String> explainedFailures) {
		StringBuilder line = new StringBuilder()
				.append(outcome.getConnectionName()).append('(').append(outcome.getConnectionId()).append(')')
				.append('.').append(outcome.getTableName()).append(": ");
		if (outcome.getError() != null) {
			line.append(ServingIndexFailures.explain(outcome.getError()));
			if (!explainedFailures.isEmpty()) {
				line.append(" | ");
			}
		}
		line.append(String.join(" | ", explainedFailures));
		return line.toString();
	}

	private static String describe(UnresolvedServingIndexTarget gap) {
		return gap.getApiName() + ": " + gap.getReason()
				+ ", connectionId = " + gap.getConnectionId()
				+ ", table = " + gap.getTableName()
				+ ", indexes = " + gap.getIndexCount();
	}

	/** 本次落地是否无需人干预（没有真失败、也没有落不了地的声明）。 */
	public boolean isClean() {
		return problems.isEmpty();
	}

	/** 一行汇总，给日志与报告用；权限不足单独点名——它是唯一「改个授权就能恢复」的常见失败。 */
	public String describe() {
		String line = "targets = " + targets + ", succeeded = " + succeeded + ", failed = " + failed
				+ ", unresolved = " + unresolved + ", created = " + created
				+ ", indexFailures = " + indexFailures + ", unsupported = " + unsupported;
		if (permissionDenied) {
			line += "; target account lacks createIndex (DDL) privilege, grant it on the target connection then re-run "
					+ "(landing is idempotent)";
		}
		return line;
	}
}
