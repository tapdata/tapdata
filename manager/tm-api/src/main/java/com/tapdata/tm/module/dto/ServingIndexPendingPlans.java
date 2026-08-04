package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 首次部署时的<b>计划来源切分</b>（纯函数）。TAP-12057 · P4-1 修订（<b>ADR-0032</b>）。
 *
 * <p>索引腿的 preview 跑在部署矩阵<b>之前</b>，而目标环境的连接由排在它<b>之后</b>的 {@code connections}
 * 腿建立。于是「首次部署到全新环境」必然出现一种中间态：<b>包里带了这个连接、目标环境还没有</b>。
 * 此前它与「包里根本没带」共用 {@code CONNECTION_UNRESOLVED} 一并报红，整条流水线在部署任何东西之前
 * 就自锁——而那恰恰是 CICD 最典型的场景。</p>
 *
 * <p><b>三态切分</b>：
 * <ol>
 *   <li>连接已落地 → 归 {@link Split#getResolved()}，交给既有 dry-run 走<b>真实比对</b>（读目标库，出 add/skip）；</li>
 *   <li>连接待落地 → 按<b>包内声明</b>直接出计划行；</li>
 *   <li>包里也没带 → 仍归 {@code resolved}，由既有逻辑照常报 {@code CONNECTION_UNRESOLVED}
 *       ——那种落不了地是真的，补不回来。</li>
 * </ol>
 *
 * <p><b>只用于 preview。</b>{@code import} 那一刻 {@code connections} 腿已经跑完，连接必须真实存在，
 * 解不出就该红（<b>ADR-0030</b>）；本切分若用在导入侧，等于把「没建成」说成「计划中」。</p>
 *
 * <p>按声明出的计划是<b>乐观</b>的：读不到目标库，就无从知道其中哪些已经存在。落地时会前置
 * {@code QueryIndexes} 自写比对并跳过已存在的，故 planned ≥ actual，以 import 报告为准。</p>
 *
 * <p>纯函数、无副作用、不触库不触 Spring：可离线 TDD。</p>
 */
public final class ServingIndexPendingPlans {

	/** 归并桶的键分隔符——用连接名/表名里不会出现的字符，避免拼接歧义。 */
	private static final String KEY_SEPARATOR = "\u0000";

	private ServingIndexPendingPlans() {
	}

	/** 切分结果：交给 dry-run 的 Module + 按声明直接出的计划行。 */
	@Getter
	public static final class Split {

		/** 连接已落地、或包里也没带（该报红的）——都由既有 dry-run 处理。 */
		private final List<ModulesDto> resolved = new ArrayList<>();

		/** 连接待落地：按包内声明出的计划行。 */
		private final List<ServingIndexPlanRow> pendingRows = new ArrayList<>();

		/**
		 * 与 {@link #pendingRows} 一一对应的手工执行语句；<b>仅 MongoDB 连接有</b>，其它数据源为空。
		 *
		 * <p>审批人常常要自己去目标库确认、或在大集合上先手工建一遍再放行；语句由 TM 生成而不是让人
		 * 照着表格手拼——手拼最容易拼错的正是方向。</p>
		 */
		private final List<String> pendingCommands = new ArrayList<>();
	}

	/**
	 * @param modules              包里带来的 Module（只看它们的 {@code servingIndexes}）
	 * @param conMap               {@code 导出侧连接 id → 目标环境连接}，即已经落地的那些
	 * @param packagedConnections  {@code 导出侧连接 id → 包里那个连接}，用于分辨「待落地」与「真的没有」
	 */
	public static Split split(List<ModulesDto> modules,
							  Map<String, DataSourceConnectionDto> conMap,
							  Map<String, DataSourceConnectionDto> packagedConnections) {
		Split split = new Split();
		if (modules == null || modules.isEmpty()) {
			return split;
		}
		// 按 (连接, 集合, 身份签名) 归并——与真实比对那条路径同一种口径
		// （ServingIndexLandingTargets 先按 (连接,集合) 分桶、ServingIndexLandingPlanner 再按签名合并）。
		// 不归并的话，两个 API 声明同一条索引就会在计划表里出现两行，审阅的人看到的条数与实际要建的对不上。
		Map<String, ServingIndexPlanRow> rowByBucket = new LinkedHashMap<>();
		Map<String, List<String>> apisByBucket = new LinkedHashMap<>();
		// 手工执行语句只对 MongoDB 生成——其它数据源的建索引语法各异，凭空生成一条
		// 可能跑不通甚至跑错的语句，比不给更糟（ServingIndexManualCommands）。
		Map<String, String> commandByBucket = new LinkedHashMap<>();
		for (ModulesDto module : modules) {
			if (module == null) {
				continue;
			}
			// 未勾选的声明不能去建（ADR-0011 D2）；一条都没勾 = 无事可做，两边都不进。
			List<ServingIndex> declared = ServingIndexes.collectedOnly(module.getServingIndexes());
			if (declared.isEmpty()) {
				continue;
			}
			String connectionId = ServingIndexLandingTargets.resolveConnectionId(module);
			if (ServingIndexLandingTargets.resolveConnection(connectionId, conMap) != null) {
				split.getResolved().add(module);
				continue;
			}
			DataSourceConnectionDto packaged = connectionId == null ? null : packagedConnections.get(connectionId);
			// 表名缺失也交回既有逻辑：所有「落不了地」的报错口径集中在一处（TABLE_NAME_MISSING）。
			if (packaged == null || isBlank(module.getTableName())) {
				split.getResolved().add(module);
				continue;
			}
			for (ServingIndex declaration : declared) {
				String bucket = packaged.getName() + KEY_SEPARATOR + module.getTableName()
						+ KEY_SEPARATOR + ServingIndexSignature.of(declaration);
				// 首个声明定下展示用的名字与 unique（名字不参与身份；unique 不一致仅注记，ADR-0005）。
				rowByBucket.computeIfAbsent(bucket, k -> row(packaged.getName(), module, declaration));
				String command = ServingIndexManualCommands.of(
						packaged.getDatabase_type(), module.getTableName(), declaration);
				if (command != null) {
					commandByBucket.putIfAbsent(bucket, command);
				}
				addApi(apisByBucket.computeIfAbsent(bucket, k -> new ArrayList<>()), module.getName());
			}
		}
		rowByBucket.forEach((bucket, row) -> {
			row.setDeclaredBy(String.join(", ", apisByBucket.get(bucket)));
			split.getPendingRows().add(row);
			String command = commandByBucket.get(bucket);
			if (command != null) {
				split.getPendingCommands().add(command);
			}
		});
		return split;
	}

	/** 同一条索引可能被多个 API 声明——都列出来，让人看清「谁需要它」；去重且保持首次出现的顺序。 */
	private static void addApi(List<String> apis, String api) {
		if (api != null && !api.isEmpty() && !apis.contains(api)) {
			apis.add(api);
		}
	}

	private static ServingIndexPlanRow row(String connectionName, ModulesDto module, ServingIndex declaration) {
		ServingIndexPlanRow row = new ServingIndexPlanRow();
		row.setConnection(connectionName);
		row.setTable(module.getTableName());
		row.setName(declaration.getName());
		// 展示串 = 身份签名，与真实比对那条路径同一种渲染（方向必须一眼可见）。
		row.setKeys(ServingIndexSignature.ofFields(declaration.getFields()));
		row.setUnique(Boolean.TRUE.equals(declaration.getUnique()));
		return row;
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
