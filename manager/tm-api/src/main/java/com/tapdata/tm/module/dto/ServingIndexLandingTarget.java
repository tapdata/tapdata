package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个落地工作项 = {@code (目标连接, 集合)} + 该桶上的声明并集。TAP-12057 · P3-1。
 *
 * <p>索引本体按集合建，所以桶必须是 {@code (连接, 集合)}——同连接不同集合不能混。连接是<b>目标环境</b>的
 * {@link DataSourceConnectionDto}（经 conMap 解析，见 {@link ServingIndexLandingTargets}）：
 * 引擎侧要拿它整体去建 PDK 连接，故这里存 DTO 本体，而非只留个 id（<b>ADR-0002</b>）。</p>
 *
 * <p>{@link #getDeclared()} 是<b>并集、未去重</b>：多 API 共表时同一字段集可能出现多条，按签名合并的口径
 * 只有一份、在 {@link ServingIndexLandingPlanner} 里。{@link #getSourceApis()} 记哪些 API 贡献了本桶
 * ——出错时能归因到 API，报告按签名回查即可（P3-2 决策：来源 API 不进逐条计划）。</p>
 */
@Getter
public class ServingIndexLandingTarget {

	/** 目标环境的连接（conMap 的值）。 */
	private final DataSourceConnectionDto connection;

	/** 目标集合名。 */
	private final String tableName;

	/** 本桶上的声明并集（只含声明项，已过 {@link ServingIndexes#collectedOnly}）；去重在 planner。 */
	private final List<ServingIndex> declared = new ArrayList<>();

	/** 贡献了本桶的 API（Module）名，按出现顺序。 */
	private final List<String> sourceApis = new ArrayList<>();

	/**
	 * 身份签名 → 声明它的 API 名。P3-2 定的口径：<b>来源 API 不进逐条计划</b>，签名即天然 join key，
	 * P3-4 出报告时按签名回查即可——所以这份归属留在工作项上，而不是塞进每条 {@link ServingIndex}。
	 */
	private final Map<String, List<String>> declaredBy = new LinkedHashMap<>();

	public ServingIndexLandingTarget(DataSourceConnectionDto connection, String tableName) {
		this.connection = connection;
		this.tableName = tableName;
	}

	/** 记一条「这个签名由哪个 API 声明」；同一签名被多个 API 声明时按出现顺序累加、不去重成一个。 */
	void recordDeclaredBy(String signature, String apiName) {
		if (signature == null || signature.isEmpty() || apiName == null || apiName.isEmpty()) {
			return;
		}
		List<String> apis = declaredBy.computeIfAbsent(signature, k -> new ArrayList<>());
		if (!apis.contains(apiName)) {
			apis.add(apiName);
		}
	}

	/** 目标环境的连接 id（十六进制）；连接或其 id 为空时返回 {@code null}。 */
	public String getConnectionId() {
		return connection == null || connection.getId() == null ? null : connection.getId().toHexString();
	}

	/** 目标环境的连接名，仅用于日志与报告。 */
	public String getConnectionName() {
		return connection == null ? null : connection.getName();
	}
}
