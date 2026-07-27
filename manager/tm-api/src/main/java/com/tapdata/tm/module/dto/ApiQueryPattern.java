package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.module.entity.Path;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 从 API 的 {@link Path} 查询形态推导「本 API 匹配」判据。TAP-12057 · P2-2（方案 §3.8.1 / §3.8.3）。
 *
 * <p>服务于默认勾选策略（{@link ServingIndexLoadPlanner}）里「匹配本 API 的 Path 查询模式」那一档：
 * 收集本 API 以<b>等值</b>查询的字段（{@code requiredQueryField} ∪ {@code where} 中 {@code eq}/{@code in}），
 * 一条索引若<b>首字段</b>命中其中之一，即认为它服务本 API 的点查（含 §3.8.1 那条「实体主键点查」的复合合并
 * 关联键索引——首字段是实体主键、尾随字段不取消匹配）。范围/排序首字段一律不算匹配（保守：留人工判断）。</p>
 *
 * <p>{@code fullCustomQuery=true} 或 {@code customWhere} 非空的 Path 已绕开条件白名单（apiserver 的
 * {@code customerQuery} 路径，方案 §2.6/§3.3），其 Path 声明不再描述真实查询，<b>整条排除</b>不参与判据。</p>
 *
 * <p><b>ENG-UNSURE</b>（Tier C，工程判断）：本判据是「智能默认勾选」的便利起点、最终以人工勾选为准
 * （§3.8.3），刻意取保守形态；判据集中在此单点，后续需要更精细（如纳入排序首字段 / ESR 前缀）时只改这里。</p>
 */
public final class ApiQueryPattern {

	/** 索引服务端能以「首字段等值」命中的运算符（去 {@code $} 前缀、小写后比对）。 */
	private static final Set<String> EQUALITY_OPS = Set.of("eq", "in");

	private final Set<String> equalityFields;

	private ApiQueryPattern(Set<String> equalityFields) {
		this.equalityFields = equalityFields;
	}

	/** 本 API 以等值查询的字段集（{@code requiredQueryField} ∪ {@code eq}/{@code in} 的 {@code where}）。 */
	public Set<String> equalityFields() {
		return equalityFields;
	}

	/** 匹配 = 索引<b>首字段</b>被本 API 以等值查询；尾随字段不取消匹配。空/无首字段 → 不匹配。 */
	public boolean matches(ServingIndex index) {
		if (index == null || index.getFields() == null || index.getFields().isEmpty()) {
			return false;
		}
		String leading = index.getFields().get(0).getField();
		return leading != null && equalityFields.contains(leading);
	}

	/** 从 Module 的全部 {@code paths} 归纳等值查询字段；跳过 custom-query 的 Path。 */
	public static ApiQueryPattern from(List<Path> paths) {
		Set<String> eq = new HashSet<>();
		if (paths != null) {
			for (Path path : paths) {
				if (path == null || isCustom(path)) {
					continue;
				}
				collectRequired(path, eq);
				collectEqualityWhere(path, eq);
			}
		}
		return new ApiQueryPattern(eq);
	}

	private static void collectRequired(Path path, Set<String> eq) {
		if (path.getRequiredQueryField() == null) {
			return;
		}
		for (Field f : path.getRequiredQueryField()) {
			if (f != null && isNotBlank(f.getFieldName())) {
				eq.add(f.getFieldName());
			}
		}
	}

	private static void collectEqualityWhere(Path path, Set<String> eq) {
		if (path.getWhere() == null) {
			return;
		}
		for (Where w : path.getWhere()) {
			if (w != null && isNotBlank(w.getFieldName()) && isEqualityOp(w.getOperator())) {
				eq.add(w.getFieldName());
			}
		}
	}

	private static boolean isCustom(Path path) {
		return Boolean.TRUE.equals(path.getFullCustomQuery()) || isNotBlank(path.getCustomWhere());
	}

	private static boolean isEqualityOp(String operator) {
		if (operator == null) {
			return false;
		}
		String norm = operator.startsWith("$") ? operator.substring(1) : operator;
		return EQUALITY_OPS.contains(norm.toLowerCase(Locale.ROOT));
	}

	private static boolean isNotBlank(String s) {
		return s != null && !s.trim().isEmpty();
	}
}
