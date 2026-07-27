package com.tapdata.tm.servingindex;

import io.tapdata.entity.schema.TapIndex;

import java.util.List;

/**
 * P2-2 · 「加载规划」端点请求体（TAP-12057，方案 §3.8）。
 *
 * <p>前端先经 P1-2 触发端点读回索引（结果经 ws 回前端，ADR-0009），再把读回的 {@code indexes} 连同
 * {@code moduleId}（路径变量）回传本端点，由 {@link ServingIndexLoadService} 做归因/默认勾选规划。</p>
 *
 * <p><b>契约为 provisional（P2-6 前端联调点）</b>：字段与端点路径以前端落盘时最终确认为准。</p>
 */
public class LoadServingIndexesRequest {

	/** 引擎读回、前端回传的该 (连接,集合) 全部物理索引。 */
	private List<TapIndex> indexes;

	public List<TapIndex> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<TapIndex> indexes) {
		this.indexes = indexes;
	}
}
