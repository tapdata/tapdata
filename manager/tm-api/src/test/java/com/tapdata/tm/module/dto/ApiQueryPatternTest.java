package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.module.entity.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2-2 · 从 API 的 {@code Path} 查询形态推导「本 API 匹配」判据（{@link ApiQueryPattern}）。
 *
 * <p>默认勾选策略（§3.8.3 行 2）里「匹配本 API 的 Path 查询模式」的判据：<b>索引的<u>首字段</u>被本 API
 * 以等值查询</b>（{@code requiredQueryField} ∪ {@code where} 中 {@code eq}/{@code in}），即认为该索引服务本 API
 * 的点查——正是 §3.8.1 那条「实体主键点查」的合并关联键索引（复合、首字段是实体主键）。<b>尾随字段不取消
 * 匹配</b>（复合索引首字段服务点查即可）；范围/排序首字段不算匹配（保守：留人工判断，§3.8.3「宁可多看一眼」）。
 * {@code fullCustomQuery=true} 或 {@code customWhere} 非空的 Path 已绕开白名单、形态不可信（§3.3），整条排除。</p>
 */
class ApiQueryPatternTest {

	private static Field field(String name) {
		Field f = new Field();
		f.setFieldName(name);
		return f;
	}

	private static com.tapdata.tm.module.dto.Where where(String fieldName, String operator) {
		com.tapdata.tm.module.dto.Where w = new com.tapdata.tm.module.dto.Where();
		w.setFieldName(fieldName);
		w.setOperator(operator);
		return w;
	}

	private static com.tapdata.tm.module.dto.Sort sort(String fieldName) {
		com.tapdata.tm.module.dto.Sort s = new com.tapdata.tm.module.dto.Sort();
		s.setFieldName(fieldName);
		return s;
	}

	/** 索引：方向对匹配无关，一律升序。 */
	private static ServingIndex idx(String... fields) {
		List<ServingIndexField> fs = new ArrayList<>();
		for (String f : fields) {
			fs.add(new ServingIndexField(f, true));
		}
		return new ServingIndex("ix", null, fs);
	}

	private static Path pathRequired(String... required) {
		Path p = new Path();
		List<Field> rq = new ArrayList<>();
		for (String r : required) {
			rq.add(field(r));
		}
		p.setRequiredQueryField(rq);
		return p;
	}

	@Test
	@DisplayName("requiredQueryField 是等值首字段：index[a] 匹配")
	void requiredQueryFieldIsEqualityLeading() {
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(pathRequired("a")));
		assertTrue(p.matches(idx("a")));
	}

	@Test
	@DisplayName("where eq → 等值首字段；where gt → 非等值：index[a] 匹配、index[b] 不匹配")
	void whereEqualityVsRange() {
		Path path = new Path();
		path.setWhere(Arrays.asList(where("a", "eq"), where("b", "gt")));
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(path));
		assertTrue(p.matches(idx("a")));
		assertFalse(p.matches(idx("b")), "范围首字段不算匹配（保守）");
	}

	@Test
	@DisplayName("where in 视作等值（B-tree 可服务）：index[a] 匹配")
	void whereInIsEquality() {
		Path path = new Path();
		path.setWhere(Collections.singletonList(where("a", "in")));
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(path));
		assertTrue(p.matches(idx("a")));
	}

	@Test
	@DisplayName("operator 归一：$eq / EQ 均识别为等值")
	void operatorNormalised() {
		Path path = new Path();
		path.setWhere(Arrays.asList(where("a", "$eq"), where("b", "EQ")));
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(path));
		assertTrue(p.matches(idx("a")));
		assertTrue(p.matches(idx("b")));
	}

	@Test
	@DisplayName("sort-only（无等值）：sort 首字段不算匹配（保守）")
	void sortOnlyDoesNotMatch() {
		Path path = new Path();
		path.setSort(Collections.singletonList(sort("a")));
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(path));
		assertFalse(p.matches(idx("a")));
	}

	@Test
	@DisplayName("§3.8.1 关键例：复合合并索引 [custId, orderDate]，API 点查 custId → 匹配（尾随字段不取消）")
	void compoundMergeIndexLeadingWithEntityPkMatches() {
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(pathRequired("custId")));
		assertTrue(p.matches(idx("custId", "orderDate")));
	}

	@Test
	@DisplayName("首字段非等值则不匹配：API 点查 custId，index[orderDate, custId] → 不匹配")
	void nonEqualityLeadingDoesNotMatch() {
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(pathRequired("custId")));
		assertFalse(p.matches(idx("orderDate", "custId")));
	}

	@Test
	@DisplayName("fullCustomQuery=true 的 Path 整条排除：其 requiredQueryField 不贡献判据")
	void fullCustomQueryPathExcluded() {
		Path path = pathRequired("a");
		path.setFullCustomQuery(true);
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(path));
		assertFalse(p.matches(idx("a")));
	}

	@Test
	@DisplayName("customWhere 非空的 Path 整条排除")
	void customWherePathExcluded() {
		Path path = pathRequired("a");
		path.setCustomWhere("{\"$where\":\"...\"}");
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(path));
		assertFalse(p.matches(idx("a")));
	}

	@Test
	@DisplayName("多 Path：匹配任一 Path 即匹配")
	void matchesAnyPath() {
		Path p1 = pathRequired("a");
		Path p2 = new Path();
		p2.setWhere(Collections.singletonList(where("b", "eq")));
		ApiQueryPattern p = ApiQueryPattern.from(Arrays.asList(p1, p2));
		assertTrue(p.matches(idx("b")));
		assertTrue(p.matches(idx("a")));
	}

	@Test
	@DisplayName("null / 空安全：无 Path、null 索引、空字段索引一律不匹配")
	void nullSafe() {
		assertFalse(ApiQueryPattern.from(null).matches(idx("a")));
		assertFalse(ApiQueryPattern.from(Collections.emptyList()).matches(idx("a")));
		ApiQueryPattern p = ApiQueryPattern.from(Collections.singletonList(pathRequired("a")));
		assertFalse(p.matches(null));
		assertFalse(p.matches(new ServingIndex("ix", null, new ArrayList<>())));
	}
}
