package com.tapdata.tm.module.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

	/**
	 * 给人手工执行的建索引语句，<b>仅 MongoDB 连接有</b>（其它数据源为空）。
	 *
	 * <p>与 {@code add} 平行的一份，不进表格：语句很长，塞进表格一列会把计划表挤垮。
	 * worker 侧渲染成表格下方的代码块。</p>
	 */
	private List<String> commands = new ArrayList<>();
}
