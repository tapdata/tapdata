package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * P2-1 · servingIndexes 写入前确定性归一化（{@link ServingIndexNormalizer}）。
 *
 * <p>身份 = 有序字段 + 方向（仅此）：排序键取「有序字段 + 方向」签名，{@code name}/{@code unique}
 * 不参与（项目红线）。复合索引内字段顺序是语义、必须保序。目的：Module JSON 跨保存不抖动，
 * 避免 CICD 全量 diff 误报（ADR-0001 follow-up）。方向语义同 P0：{@code asc==FALSE→-1}，
 * {@code TRUE/null→1}。</p>
 */
class ServingIndexNormalizerTest {

	private static ServingIndexField f(String field, Boolean asc) {
		return new ServingIndexField(field, asc);
	}

	private static ServingIndex idx(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	/** 把归一化结果映射为可读的「有序字段+方向」签名列表，便于断言定序与保序。 */
	private static List<String> signatures(List<ServingIndex> list) {
		List<String> out = new ArrayList<>();
		for (ServingIndex si : list) {
			StringBuilder sb = new StringBuilder();
			for (ServingIndexField sf : si.getFields()) {
				if (sb.length() > 0) sb.append(',');
				sb.append(sf.getField()).append(':').append(Boolean.FALSE.equals(sf.getAsc()) ? -1 : 1);
			}
			out.add(sb.toString());
		}
		return out;
	}

	@Test
	@DisplayName("外层列表按「有序字段+方向」签名确定性排序（与输入序无关）")
	void sortsOuterListDeterministically() {
		List<ServingIndex> a = ServingIndexNormalizer.normalize(new ArrayList<>(Arrays.asList(
				idx("ix_b", null, f("b", null)),
				idx("ix_a", null, f("a", null))
		)));
		List<ServingIndex> b = ServingIndexNormalizer.normalize(new ArrayList<>(Arrays.asList(
				idx("ix_a", null, f("a", null)),
				idx("ix_b", null, f("b", null))
		)));
		assertEquals(Arrays.asList("a:1", "b:1"), signatures(a));
		assertEquals(signatures(a), signatures(b), "两种输入序归一化后应同序（确定性）");
	}

	@Test
	@DisplayName("复合索引内字段顺序是语义，保序不重排")
	void preservesCompoundFieldOrder() {
		List<ServingIndex> out = ServingIndexNormalizer.normalize(new ArrayList<>(Collections.singletonList(
				idx("ix", null, f("a", true), f("b", false))
		)));
		assertEquals(Collections.singletonList("a:1,b:-1"), signatures(out));
	}

	@Test
	@DisplayName("方向进入签名：{a:desc} 与 {a:asc} 视为不同、确定性定序（desc 在前）")
	void directionAffectsSignature() {
		List<ServingIndex> out = ServingIndexNormalizer.normalize(new ArrayList<>(Arrays.asList(
				idx("ix_asc", null, f("a", true)),
				idx("ix_desc", null, f("a", false))
		)));
		// 签名字符串 "a:-1" < "a:1"（'-' < '1'）→ desc 在前，确定性
		assertEquals(Arrays.asList("a:-1", "a:1"), signatures(out));
	}

	@Test
	@DisplayName("null/空安全：null→null（不给无索引的 Module 塞空数组），空→空")
	void nullSafe() {
		assertNull(ServingIndexNormalizer.normalize(null));
		assertEquals(Collections.emptyList(), ServingIndexNormalizer.normalize(new ArrayList<>()));
	}

	@Test
	@DisplayName("方向规范为显式布尔：升序(含 null)→TRUE、降序→FALSE，防 JSON 表示漂移致 diff 抖动")
	void canonicalizesDirectionToExplicitBoolean() {
		List<ServingIndex> out = ServingIndexNormalizer.normalize(new ArrayList<>(Collections.singletonList(
				idx("ix", null, f("a", null), f("b", false), f("c", true))
		)));
		List<ServingIndexField> fields = out.get(0).getFields();
		assertEquals(Boolean.TRUE, fields.get(0).getAsc(), "null 升序应规范为显式 TRUE");
		assertEquals(Boolean.FALSE, fields.get(1).getAsc());
		assertEquals(Boolean.TRUE, fields.get(2).getAsc());
	}

	@Test
	@DisplayName("返回深副本、不改动入参（save 链路不应有副作用）")
	void doesNotMutateInput() {
		ServingIndexField rawField = f("a", null);
		ServingIndex original = idx("ix", null, rawField);
		List<ServingIndex> input = new ArrayList<>(Collections.singletonList(original));

		List<ServingIndex> out = ServingIndexNormalizer.normalize(input);

		assertSame(original, input.get(0), "入参列表元素不应被替换");
		assertNull(rawField.getAsc(), "入参字段的方向不应被就地规范");
		assertEquals(Boolean.TRUE, out.get(0).getFields().get(0).getAsc(), "副本里方向已规范");
	}
}
