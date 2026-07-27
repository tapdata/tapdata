package com.tapdata.tm.module.dto;

import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2-2 · 读回索引 {@code TapIndex} → 平台声明 {@link ServingIndex} 的映射 + 保真闸（{@link ServingIndexMapper}）。
 *
 * <p><b>映射</b>：{@code name}/{@code unique}/有序字段（含方向 {@code fieldAsc→asc}）忠实复制；方向不在此规范
 * （规范化在 save 链路 {@link ServingIndexNormalizer}）。<b>保真闸</b>（ADR-0004 / §3.8.2）：读回投影里带 MongoDB
 * 保留字段名（{@code _fts}/{@code _ftsx}=text、{@code $**}=wildcard）者判「超出支持范围」→ 不支持、仅报告不采纳；
 * 其余（含 {@code _id_} 及读不出属性的 partial/TTL）按普通索引支持（静默收录，已知残余风险）。</p>
 */
class ServingIndexMapperTest {

	private static TapIndexField tif(String name, Boolean asc) {
		return new TapIndexField().name(name).fieldAsc(asc);
	}

	private static TapIndex tapIndex(String name, Boolean unique, TapIndexField... fields) {
		TapIndex idx = new TapIndex().name(name);
		if (unique != null) {
			idx.unique(unique);
		}
		for (TapIndexField f : fields) {
			idx.indexField(f);
		}
		return idx;
	}

	@Test
	@DisplayName("映射单字段：name/unique/字段+方向忠实复制")
	void mapsSingleField() {
		ServingIndex si = ServingIndexMapper.toServingIndex(tapIndex("ix_a", true, tif("a", true)));
		assertEquals("ix_a", si.getName());
		assertEquals(Boolean.TRUE, si.getUnique());
		assertEquals(1, si.getFields().size());
		assertEquals("a", si.getFields().get(0).getField());
		assertEquals(Boolean.TRUE, si.getFields().get(0).getAsc());
	}

	@Test
	@DisplayName("映射复合索引：字段顺序与各自方向保真（fieldAsc==false→asc==false）")
	void mapsCompoundPreservingOrderAndDirection() {
		ServingIndex si = ServingIndexMapper.toServingIndex(tapIndex("ix", null, tif("a", true), tif("b", false)));
		assertEquals("a", si.getFields().get(0).getField());
		assertEquals(Boolean.TRUE, si.getFields().get(0).getAsc());
		assertEquals("b", si.getFields().get(1).getField());
		assertEquals(Boolean.FALSE, si.getFields().get(1).getAsc());
	}

	@Test
	@DisplayName("方向不在映射层规范：fieldAsc==null → asc==null（规范化留给 save 链路）")
	void doesNotCanonicalizeDirectionAtMapping() {
		ServingIndex si = ServingIndexMapper.toServingIndex(tapIndex("ix", null, tif("a", null)));
		assertNull(si.getFields().get(0).getAsc());
	}

	@Test
	@DisplayName("保真闸：普通索引 → 支持")
	void supportsNormalIndex() {
		assertTrue(ServingIndexMapper.isSupported(tapIndex("ix_a", false, tif("a", true))));
	}

	@Test
	@DisplayName("保真闸：_id_ 默认索引 → 支持（是普通索引、不被拒；系统性由 planner 处理）")
	void supportsIdIndex() {
		assertTrue(ServingIndexMapper.isSupported(tapIndex("_id_", false, tif("_id", true))));
	}

	@Test
	@DisplayName("保真闸：text 索引（字段 _fts / _ftsx）→ 不支持（超出范围）")
	void rejectsTextIndex() {
		assertFalse(ServingIndexMapper.isSupported(tapIndex("t_txt", false, tif("_fts", true), tif("_ftsx", true))));
	}

	@Test
	@DisplayName("保真闸：wildcard 索引（字段 $**）→ 不支持")
	void rejectsWildcardIndex() {
		assertFalse(ServingIndexMapper.isSupported(tapIndex("w", false, tif("$**", true))));
	}

	@Test
	@DisplayName("保真闸：wildcard 子路径（field path.$**）→ 不支持")
	void rejectsWildcardSubpathIndex() {
		assertFalse(ServingIndexMapper.isSupported(tapIndex("w2", false, tif("attrs.$**", true))));
	}
}
