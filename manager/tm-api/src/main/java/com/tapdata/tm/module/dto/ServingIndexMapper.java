package com.tapdata.tm.module.dto;

import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;

import java.util.ArrayList;
import java.util.List;

/**
 * 读回索引 {@link TapIndex} → 平台声明 {@link ServingIndex} 的映射 + 保真闸。TAP-12057 · P2-2。
 *
 * <p><b>映射</b>：{@code name} / {@code unique} / 有序字段（{@code fieldAsc → asc}）忠实复制；方向<b>不</b>在此
 * 规范（{@code null} 保留），规范化交给 save 链路 {@link ServingIndexNormalizer}——加载展示忠实于读回形态。</p>
 *
 * <p><b>保真闸</b>（<b>ADR-0004</b> / 方案 §3.8.2）：连接器只吐 {@code name}/{@code key}/{@code unique} 的有损投影，
 * 读不到 partial/TTL/sparse/collation。能拦的只有投影层面认得出的一类——字段名带 MongoDB 保留标记
 * （{@code _fts}/{@code _ftsx} = text、{@code $**} = wildcard）→ 判「超出支持范围」、{@link #isSupported} 返回
 * {@code false}（仅报告、不采纳）。其余（含 {@code _id_}，以及属性被投影丢弃的 partial/TTL）一律按普通索引支持
 * ——静默收录，已知残余风险（ADR-0004 已接受）。</p>
 */
public final class ServingIndexMapper {

	private ServingIndexMapper() {
	}

	/** {@code TapIndex → ServingIndex}：字段与方向忠实复制、不规范方向。{@code null → null}。 */
	public static ServingIndex toServingIndex(TapIndex tapIndex) {
		if (tapIndex == null) {
			return null;
		}
		List<ServingIndexField> fields = null;
		if (tapIndex.getIndexFields() != null) {
			fields = new ArrayList<>(tapIndex.getIndexFields().size());
			for (TapIndexField f : tapIndex.getIndexFields()) {
				fields.add(new ServingIndexField(f.getName(), f.getFieldAsc()));
			}
		}
		return new ServingIndex(tapIndex.getName(), tapIndex.getUnique(), fields);
	}

	/**
	 * 保真闸：索引是否在本期支持范围内。仅当能从有损投影里认出保留标记（text/wildcard）时返回 {@code false}；
	 * 读不出的属性（partial/TTL/…）无从拦截，按普通索引支持（残余风险，ADR-0004）。
	 */
	public static boolean isSupported(TapIndex tapIndex) {
		if (tapIndex == null || tapIndex.getIndexFields() == null) {
			return true;
		}
		for (TapIndexField f : tapIndex.getIndexFields()) {
			if (isReservedMarker(f.getName())) {
				return false;
			}
		}
		return true;
	}

	/** MongoDB text（{@code _fts}/{@code _ftsx}）/ wildcard（{@code $**}，含子路径 {@code path.$**}）保留标记。 */
	private static boolean isReservedMarker(String fieldName) {
		if (fieldName == null) {
			return false;
		}
		return "_fts".equals(fieldName) || "_ftsx".equals(fieldName) || fieldName.contains("$**");
	}
}
