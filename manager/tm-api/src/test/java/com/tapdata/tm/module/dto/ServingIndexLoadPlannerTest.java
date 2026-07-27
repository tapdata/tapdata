package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.module.entity.Path;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2-2 · 加载默认勾选策略（{@link ServingIndexLoadPlanner}），方案 §3.8.1 / §3.8.3。
 *
 * <p><b>全表可见、不预过滤</b>（§3.8.1）：输出一条不少（含 {@code _id_}、超范围项），归因只影响
 * 「可勾 / 默认勾」、绝不隐藏。默认勾选<b>自上而下、首个命中即定</b>（§3.8.3）：</p>
 * <ol>
 *   <li>{@code _id_} / text / wildcard → 灰置不可勾</li>
 *   <li>匹配本 API 的 Path 查询模式 → 默认勾</li>
 *   <li>上次已收录进本 API → 默认勾（保持）</li>
 *   <li>已被别的 API 收录 → 默认不勾（标注「已被 API X 收录」）</li>
 *   <li>其余 → 默认不勾（可见可手勾）</li>
 * </ol>
 */
class ServingIndexLoadPlannerTest {

	private static TapIndex tapIndex(String name, Boolean unique, String... ascFields) {
		TapIndex idx = new TapIndex().name(name);
		if (unique != null) {
			idx.unique(unique);
		}
		for (String f : ascFields) {
			idx.indexField(new TapIndexField().name(f).fieldAsc(true));
		}
		return idx;
	}

	private static ApiQueryPattern patternRequiring(String... required) {
		Path p = new Path();
		List<Field> rq = new ArrayList<>();
		for (String r : required) {
			Field f = new Field();
			f.setFieldName(r);
			rq.add(f);
		}
		p.setRequiredQueryField(rq);
		return ApiQueryPattern.from(Collections.singletonList(p));
	}

	private static ApiQueryPattern emptyPattern() {
		return ApiQueryPattern.from(Collections.emptyList());
	}

	/** 单条规划的便捷入口。 */
	private static LoadedServingIndex planOne(TapIndex index, ApiQueryPattern pattern,
											  Set<String> thisApi, Map<String, String> otherApi) {
		List<LoadedServingIndex> out = ServingIndexLoadPlanner.plan(
				Collections.singletonList(index), pattern, thisApi, otherApi);
		assertEquals(1, out.size());
		return out.get(0);
	}

	@Test
	@DisplayName("行1：_id_ → SYSTEM_INDEX，不可勾、不默认勾")
	void idIndexIsSystemUncheckable() {
		LoadedServingIndex r = planOne(tapIndex("_id_", false, "_id"),
				emptyPattern(), Collections.emptySet(), Collections.emptyMap());
		assertEquals(LoadedIndexAttribution.SYSTEM_INDEX, r.getAttribution());
		assertFalse(r.isCheckable());
		assertFalse(r.isDefaultChecked());
	}

	@Test
	@DisplayName("行1：text 索引（_fts）→ UNSUPPORTED，不可勾、不默认勾（超范围仅报告）")
	void textIndexUnsupportedUncheckable() {
		LoadedServingIndex r = planOne(tapIndex("t_txt", false, "_fts", "_ftsx"),
				emptyPattern(), Collections.emptySet(), Collections.emptyMap());
		assertEquals(LoadedIndexAttribution.UNSUPPORTED, r.getAttribution());
		assertFalse(r.isCheckable());
		assertFalse(r.isDefaultChecked());
	}

	@Test
	@DisplayName("行1：wildcard 索引（$**）→ UNSUPPORTED，不可勾")
	void wildcardIndexUnsupported() {
		LoadedServingIndex r = planOne(tapIndex("w", false, "$**"),
				emptyPattern(), Collections.emptySet(), Collections.emptyMap());
		assertEquals(LoadedIndexAttribution.UNSUPPORTED, r.getAttribution());
		assertFalse(r.isCheckable());
	}

	@Test
	@DisplayName("行2：匹配本 API 的 Path 查询 → MATCHES_API，可勾、默认勾")
	void matchesApiDefaultChecked() {
		LoadedServingIndex r = planOne(tapIndex("ix_cust", false, "custId"),
				patternRequiring("custId"), Collections.emptySet(), Collections.emptyMap());
		assertEquals(LoadedIndexAttribution.MATCHES_API, r.getAttribution());
		assertTrue(r.isCheckable());
		assertTrue(r.isDefaultChecked());
	}

	@Test
	@DisplayName("行3：上次已收录进本 API（签名命中、不匹配当前 Path）→ COLLECTED_BY_THIS_API，默认勾（保持）")
	void alreadyThisApiDefaultChecked() {
		Set<String> thisApi = new HashSet<>(Collections.singletonList("legacyField:1"));
		LoadedServingIndex r = planOne(tapIndex("ix_legacy", false, "legacyField"),
				emptyPattern(), thisApi, Collections.emptyMap());
		assertEquals(LoadedIndexAttribution.COLLECTED_BY_THIS_API, r.getAttribution());
		assertTrue(r.isCheckable());
		assertTrue(r.isDefaultChecked());
	}

	@Test
	@DisplayName("行4：已被别的 API 收录 → COLLECTED_BY_OTHER_API，默认不勾、带来源 API 名")
	void alreadyOtherApiNotCheckedButLabeled() {
		Map<String, String> otherApi = new HashMap<>();
		otherApi.put("sharedField:1", "OrderQueryApi");
		LoadedServingIndex r = planOne(tapIndex("ix_shared", false, "sharedField"),
				emptyPattern(), Collections.emptySet(), otherApi);
		assertEquals(LoadedIndexAttribution.COLLECTED_BY_OTHER_API, r.getAttribution());
		assertTrue(r.isCheckable());
		assertFalse(r.isDefaultChecked());
		assertEquals("OrderQueryApi", r.getAttributionApi());
	}

	@Test
	@DisplayName("行5：纯流水线/手工建但不匹配 → UNCLASSIFIED，可勾但默认不勾")
	void unclassifiedVisibleUnchecked() {
		LoadedServingIndex r = planOne(tapIndex("ix_pipeline", false, "__tapd8s_update_cond"),
				patternRequiring("custId"), Collections.emptySet(), Collections.emptyMap());
		assertEquals(LoadedIndexAttribution.UNCLASSIFIED, r.getAttribution());
		assertTrue(r.isCheckable());
		assertFalse(r.isDefaultChecked());
	}

	@Test
	@DisplayName("首个命中即定：匹配本 API 优先于「已被别的 API 收录」→ MATCHES_API、默认勾")
	void matchesApiWinsOverOtherApi() {
		Map<String, String> otherApi = new HashMap<>();
		otherApi.put("custId:1", "OtherApi");
		LoadedServingIndex r = planOne(tapIndex("ix_cust", false, "custId"),
				patternRequiring("custId"), Collections.emptySet(), otherApi);
		assertEquals(LoadedIndexAttribution.MATCHES_API, r.getAttribution());
		assertTrue(r.isDefaultChecked());
	}

	@Test
	@DisplayName("首个命中即定：_id_ 优先于一切（即便其签名恰在别的 API 收录集里，仍判系统、不可勾）")
	void idIndexWinsOverEverything() {
		Map<String, String> otherApi = new HashMap<>();
		otherApi.put("_id:1", "SomeApi");
		LoadedServingIndex r = planOne(tapIndex("_id_", false, "_id"),
				patternRequiring("_id"), Collections.emptySet(), otherApi);
		assertEquals(LoadedIndexAttribution.SYSTEM_INDEX, r.getAttribution());
		assertFalse(r.isCheckable());
	}

	@Test
	@DisplayName("全表可见不预过滤：输出条数 == 输入条数、保序，一条不少")
	void allIndexesVisibleInOrder() {
		List<TapIndex> input = Arrays.asList(
				tapIndex("_id_", false, "_id"),
				tapIndex("ix_cust", false, "custId"),
				tapIndex("t_txt", false, "_fts"));
		List<LoadedServingIndex> out = ServingIndexLoadPlanner.plan(
				input, patternRequiring("custId"), Collections.emptySet(), Collections.emptyMap());
		assertEquals(3, out.size());
		assertEquals(LoadedIndexAttribution.SYSTEM_INDEX, out.get(0).getAttribution());
		assertEquals(LoadedIndexAttribution.MATCHES_API, out.get(1).getAttribution());
		assertEquals(LoadedIndexAttribution.UNSUPPORTED, out.get(2).getAttribution());
	}

	@Test
	@DisplayName("映射保真：输出携带 name/unique/字段供展示与采纳")
	void carriesMappedIndexForDisplay() {
		LoadedServingIndex r = planOne(tapIndex("ix_cust", true, "custId"),
				patternRequiring("custId"), Collections.emptySet(), Collections.emptyMap());
		assertEquals("ix_cust", r.getIndex().getName());
		assertEquals(Boolean.TRUE, r.getIndex().getUnique());
		assertEquals("custId", r.getIndex().getFields().get(0).getField());
	}

	@Test
	@DisplayName("null 安全：null 索引列表 → 空结果")
	void nullSafe() {
		assertTrue(ServingIndexLoadPlanner.plan(null, emptyPattern(),
				Collections.emptySet(), Collections.emptyMap()).isEmpty());
	}
}
