package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * P2-2 · 服务型索引「身份签名」（{@link ServingIndexSignature}）。
 *
 * <p>身份 = 有序字段 + 方向（仅此，项目红线）。签名是加载侧比对（「已收录进本/他 API」）与 P2-1 归一化
 * 定序<b>共用</b>的唯一身份定义，避免两处各写一份而漂移。方向语义同 P0：{@code asc==FALSE→-1}，
 * {@code TRUE/null→1}；{@code name}/{@code unique} 绝不进签名。</p>
 */
class ServingIndexSignatureTest {

	private static ServingIndexField f(String field, Boolean asc) {
		return new ServingIndexField(field, asc);
	}

	private static ServingIndex idx(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	@Test
	@DisplayName("签名 = 「字段:方向」逗号拼接；asc→1、desc→-1")
	void encodesFieldsAndDirection() {
		assertEquals("a:1,b:-1", ServingIndexSignature.of(idx("ix", null, f("a", true), f("b", false))));
	}

	@Test
	@DisplayName("null 方向归升序（同 P0）：asc==null → :1")
	void nullDirectionIsAscending() {
		assertEquals("a:1", ServingIndexSignature.of(idx("ix", null, f("a", null))));
	}

	@Test
	@DisplayName("name / unique 不进签名（身份仅有序字段+方向）")
	void nameAndUniqueDoNotAffectSignature() {
		String a = ServingIndexSignature.of(idx("ix_name_one", true, f("a", true)));
		String b = ServingIndexSignature.of(idx("totally_different", null, f("a", true)));
		assertEquals(a, b, "名字与 unique 不同、字段+方向相同 → 同签名");
	}

	@Test
	@DisplayName("复合索引字段顺序是语义：{a,b} 与 {b,a} 签名不同")
	void compoundFieldOrderIsSignificant() {
		String ab = ServingIndexSignature.of(idx("ix", null, f("a", true), f("b", true)));
		String ba = ServingIndexSignature.of(idx("ix", null, f("b", true), f("a", true)));
		assertNotEquals(ab, ba);
	}

	@Test
	@DisplayName("null / 空字段安全：均返回空签名")
	void nullSafe() {
		assertEquals("", ServingIndexSignature.of(null));
		assertEquals("", ServingIndexSignature.of(new ServingIndex(null, null, null)));
		assertEquals("", ServingIndexSignature.of(idx("ix", null)));
	}
}
