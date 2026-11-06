package com.tapdata.tm.servingindex;

import io.tapdata.entity.schema.TapIndex;

import java.util.Collections;
import java.util.List;

/**
 * 一次「按连接 + 表读回索引」的结果。TAP-12057 · P3-3（ADR-0012 D1）。
 *
 * <p>三态刻意分开，<b>不能合并</b>：{@code 成功}（拿到目标现有索引）、{@code 失败}（引擎报错）、
 * {@code 超时}（引擎不在线 / 消息没回来）。后两者若被当成「目标没有索引」，比对就会认为全都要建
 * ——在只加不删的语义下，那是往目标库里重复建索引。</p>
 */
public final class ServingIndexReadback {

	private final List<TapIndex> indexes;
	private final String error;
	private final boolean timedOut;

	private ServingIndexReadback(List<TapIndex> indexes, String error, boolean timedOut) {
		this.indexes = indexes;
		this.error = error;
		this.timedOut = timedOut;
	}

	public static ServingIndexReadback success(List<TapIndex> indexes) {
		return new ServingIndexReadback(indexes == null ? Collections.emptyList() : indexes, null, false);
	}

	public static ServingIndexReadback failed(String error) {
		return new ServingIndexReadback(Collections.emptyList(), error, false);
	}

	public static ServingIndexReadback timeout() {
		return new ServingIndexReadback(Collections.emptyList(), null, true);
	}

	/** 目标集合上现有的全部物理索引；失败/超时时为空列表。 */
	public List<TapIndex> getIndexes() {
		return indexes;
	}

	/** 引擎侧错误信息；成功或超时为 {@code null}。 */
	public String getError() {
		return error;
	}

	public boolean isTimedOut() {
		return timedOut;
	}

	/** 是否可用于比对——只有成功才行（见类注释）。 */
	public boolean isUsable() {
		return !timedOut && error == null;
	}
}
