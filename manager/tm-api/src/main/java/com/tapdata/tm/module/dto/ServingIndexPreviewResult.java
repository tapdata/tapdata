package com.tapdata.tm.module.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * {@code POST /api/groupInfo/preview/indexes} 的响应体。TAP-12057 · P4-1（方案 §3.5）。
 *
 * <p><b>计划表在顶层</b>（继承 {@link ServingIndexPlanDiff}）：worker 的 {@code preview-resource.sh}
 * 读的是 {@code .data.add / .update / .delete}，它是所有资源类型共用的通用脚本，P4-2 只该加一条 URL 分支。</p>
 *
 * <p>{@link #report} 是 P3-4 那份分桶报告全文（将创建 / 将跳过 / 目标多出 + 索引总数水位 + 逐条来源 API）。
 * 通用脚本不认识它，但会把整个响应体打进 CI 日志——所以它既是给人看的详情，也是将来 worker
 * 想渲染得更细时现成的输入。</p>
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ServingIndexPreviewResult extends ServingIndexPlanDiff implements Serializable {

	private static final long serialVersionUID = 1L;

	/** dry-run 分桶报告全文（P3-4）。 */
	private ServingIndexLandingReport report;
}
