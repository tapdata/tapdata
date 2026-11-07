package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * {@code POST /api/groupInfo/import/indexes} 的响应体。TAP-12057 · P4-1（方案 §3.5）。
 *
 * <p><b>计划表嵌在 {@link #diff} 里</b>，与其余导入腿的 {@code GroupImportResult} 同形——worker 的
 * {@code import-resource.sh} 读 {@code .data.diff} 并把它整串写进 {@code GITHUB_OUTPUT} 供下游 job 用。
 * 富报告因此<b>刻意放在 diff 之外</b>：塞进去会让每次部署往 GITHUB_OUTPUT 里灌一份全量报告。</p>
 *
 * <p>{@link ServingIndexPlanDiff#getAdd()} 在这里是<b>真建成的</b>那些，不是计划的那些（ADR-0013）。</p>
 */
@Data
public class ServingIndexImportResult implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 实际发生的变更（add = 真建成的索引；update/delete 恒空）。 */
	private ServingIndexPlanDiff diff = new ServingIndexPlanDiff();

	/** 落地报告全文，含逐 target 结果与汇总（P3-4 / P3-5）。 */
	private ServingIndexLandingReport report;
}
