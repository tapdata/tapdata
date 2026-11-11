package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * P3-2 · 落地建索引的确定性取名（{@link ServingIndexName}）。
 *
 * <p>方案 §3.4「建索引的取名」：名字不参与匹配、也无保留价值，落地一律用<b>由有序字段+方向确定性推导</b>
 * 的 MongoDB 默认名（{@code field_dir_…}）。要钉住的两条性质：<b>同字段集必同名</b>（天然幂等）、
 * <b>不同字段集必不同名</b>（多 API 共表永不撞名）。声明里带的原始名只是展示标签，绝不用于建索引。</p>
 */
class ServingIndexNameTest {

	private static ServingIndexField f(String field, Boolean asc) {
		return new ServingIndexField(field, asc);
	}

	private static ServingIndex idx(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	@Test
	@DisplayName("默认名 = MongoDB 依 key 生成的 field_dir 串：{a:1,b:-1} → a_1_b_-1")
	void derivesMongoDefaultName() {
		assertEquals("a_1_b_-1", ServingIndexName.of(idx("人起的名", null, f("a", true), f("b", false))));
	}

	@Test
	@DisplayName("null 方向归升序（同 P0）：{a:null} → a_1")
	void nullDirectionIsAscending() {
		assertEquals("a_1", ServingIndexName.of(idx("ix", null, f("a", null))));
	}

	@Test
	@DisplayName("嵌套字段路径原样进名字：{profile.city:1} → profile.city_1")
	void keepsDottedFieldPath() {
		assertEquals("profile.city_1", ServingIndexName.of(idx("ix", null, f("profile.city", true))));
	}

	@Test
	@DisplayName("同「有序字段+方向」必得同名（跨环境稳定、天然幂等），声明名与 unique 不影响")
	void sameIdentitySameName() {
		String a = ServingIndexName.of(idx("ix_from_prod", true, f("a", true), f("b", false)));
		String b = ServingIndexName.of(idx("完全不同的名字", null, f("a", true), f("b", false)));
		assertNotNull(a, "有字段就必须推出名字（否则本断言形同虚设）");
		assertNotEquals("", a);
		assertEquals(a, b);
	}

	@Test
	@DisplayName("不同「有序字段+方向」必得不同名（多 API 共表永不撞名）：方向/字段序都算不同")
	void differentIdentityDifferentName() {
		String asc = ServingIndexName.of(idx("ix", null, f("a", true)));
		String desc = ServingIndexName.of(idx("ix", null, f("a", false)));
		assertNotEquals(asc, desc, "方向不同 → 名字必须不同");

		String ab = ServingIndexName.of(idx("ix", null, f("a", true), f("b", true)));
		String ba = ServingIndexName.of(idx("ix", null, f("b", true), f("a", true)));
		assertNotEquals(ab, ba, "复合字段顺序是语义 → 名字必须不同");
	}

	@Test
	@DisplayName("null / 空字段安全：返回空名（调用方不得据此建索引）")
	void nullSafe() {
		assertEquals("", ServingIndexName.of(null));
		assertEquals("", ServingIndexName.of(new ServingIndex(null, null, null)));
		assertEquals("", ServingIndexName.of(idx("ix", null)));
	}
}
