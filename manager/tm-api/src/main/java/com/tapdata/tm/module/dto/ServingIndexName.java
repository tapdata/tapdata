package com.tapdata.tm.module.dto;

/**
 * 落地建索引时的确定性取名。TAP-12057 · P3-2（方案 §3.4「建索引的取名」）。
 *
 * <p>索引名<b>不参与</b>身份比对、也无保留价值（§3.8.4），所以落地一律<b>不用</b>声明里带的原始名
 * （那只是加载时从源环境读回的展示标签），而用<b>由有序字段+方向推导</b>的 MongoDB 默认名
 * ——即 MongoDB 自己依 {@code key} 生成的形态，{@code {a:1,b:-1}} → {@code a_1_b_-1}。</p>
 *
 * <p>两条性质正是这么取名的理由：<b>同字段集必得同名</b>（跨环境天然稳定 → 幂等）、
 * <b>不同字段集必得不同名</b>（多个 API 共表永不撞名，消掉「同名异定义被连接器 errorCode 86 吞掉、
 * 落败方永不建」那个坑）。与身份签名 {@link ServingIndexSignature} 同源同料（有序字段 + 方向）、
 * 只是编码不同：签名用于配对，本类用于建索引取名。</p>
 *
 * <p>方向语义同 P0：{@code asc==FALSE→-1}，{@code TRUE/null→1}。</p>
 */
public final class ServingIndexName {

	private ServingIndexName() {
	}

	/** 由「有序字段 + 方向」推导的 MongoDB 默认索引名；{@code null}/空字段返回空串（调用方不得据此建索引）。 */
	public static String of(ServingIndex index) {
		if (index == null || index.getFields() == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (ServingIndexField f : index.getFields()) {
			if (sb.length() > 0) {
				sb.append('_');
			}
			sb.append(f.getField()).append('_').append(Boolean.FALSE.equals(f.getAsc()) ? -1 : 1);
		}
		return sb.toString();
	}
}
