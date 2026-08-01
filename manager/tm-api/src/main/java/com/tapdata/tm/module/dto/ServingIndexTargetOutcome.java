package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个 {@code (连接, 集合)} 落地的结果。TAP-12057 · P3-3。
 *
 * <p>逐 target 记账，<b>不首错即停</b>：一个集合读不回来或建失败，其余集合照做，最后一并汇总（P3-5）。
 * P3-4 的 dry-run 分桶报告直接读这里的 {@link #getPlan()}。</p>
 *
 * <p>{@code plan} 为 {@code null} 表示<b>连比对都没做成</b>（读回失败/超时）——这与「比对了、无需创建」
 * 是两回事，不能混：前者是失败，后者是幂等成功。</p>
 */
@Data
public class ServingIndexTargetOutcome implements Serializable {

	private static final long serialVersionUID = 1L;

	private String connectionId;

	private String connectionName;

	private String tableName;

	/** 贡献本桶的 API 名。 */
	private List<String> sourceApis = new ArrayList<>();

	/** 三桶比对结果；读回失败时为 {@code null}。 */
	private ServingIndexLandingPlan plan;

	/** 实际建成的索引名。 */
	private List<String> created = new ArrayList<>();

	/** 逐条失败，形如 {@code "a_1: <原因>"}。 */
	private List<String> failed = new ArrayList<>();

	/** 本 target 整体失败原因（读回失败 / 建索引整体失败 / 超时）；成功为 {@code null}。 */
	private String error;

	/** 本 target 是否成功走完（读回 → 比对 →（按需）创建，且无逐条失败）。 */
	public boolean isSucceeded() {
		return error == null && plan != null && failed.isEmpty();
	}
}
