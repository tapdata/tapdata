package com.tapdata.tm.module.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link ServingIndex} 的「是否声明」口径。TAP-12057。
 *
 * <p>Module 的 {@code servingIndexes} 里同时存着<b>声明项</b>与<b>只是读回后留着的</b>
 * （见 {@link ServingIndex#getCollected()}）。凡是要回答「本 API 到底要哪些索引」的地方
 * ——部署（P3 创建）、归因（已被本/他 API 收录）、导出比对——都必须先过这里，
 * 免得各处对 {@code null} 的解释各写一遍、各错一遍。</p>
 */
public final class ServingIndexes {

	private ServingIndexes() {
	}

	/** 是否为声明项；{@code null} 按 {@code true}（历史数据只存勾选项，语义须保持不变）。 */
	public static boolean isCollected(ServingIndex index) {
		return index != null && !Boolean.FALSE.equals(index.getCollected());
	}

	/**
	 * 是否为 MongoDB 自建的 {@code _id_} 默认索引（按名判定，保留名）。
	 *
	 * <p>它<b>不由本功能管理</b>：加载侧灰置不可勾、不入库（P2），落地侧也不该出现在「目标多出」的展示里
	 * ——那一桶是给人看「要不要处理」的，而 {@code _id_} 既删不得也声明不得。但它<b>占着 Mongo 单集合
	 * 64 索引上限里的一格</b>，所以只从展示里去掉、计数照算（ADR-0008 的预检不能少算）。</p>
	 */
	public static boolean isDefaultIdIndex(ServingIndex index) {
		return index != null && "_id_".equals(index.getName());
	}

	/** 仅保留声明项，保持入参顺序。 */
	public static List<ServingIndex> collectedOnly(List<ServingIndex> indexes) {
		if (indexes == null) {
			return Collections.emptyList();
		}
		List<ServingIndex> out = new ArrayList<>(indexes.size());
		for (ServingIndex index : indexes) {
			if (isCollected(index)) {
				out.add(index);
			}
		}
		return out;
	}
}
