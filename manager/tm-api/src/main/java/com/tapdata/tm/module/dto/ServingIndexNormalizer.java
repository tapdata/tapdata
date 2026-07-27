package com.tapdata.tm.module.dto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 服务型索引写入前的确定性归一化。TAP-12057 · P2-1（见 <b>ADR-0001</b> follow-up）。
 *
 * <p>把一个 Module 的 {@code servingIndexes} 列表归一为稳定形态，使 Module JSON 跨保存不抖动，避免
 * CICD 全量 diff（{@code normalizeModuleForComparison} 黑名单式全量比对）因表示差异误报变更。两件事：</p>
 * <ol>
 *   <li><b>确定性排序</b>：按「有序字段 + 方向」签名重排<b>外</b>层索引列表；排序键只取有序字段 + 方向，
 *       {@code name}/{@code unique} 不参与（项目红线 —— 索引身份仅此）。复合索引<b>内</b>字段顺序是语义、保序。</li>
 *   <li><b>方向规范为显式布尔</b>：{@code asc == FALSE → 降序(FALSE)}，{@code TRUE / null → 升序(TRUE)}
 *       （同 P0：null 归升序）。否则「升序」既可写成 {@code asc:true} 又可省略 {@code asc}，同一索引两种 JSON、diff 误抖。</li>
 * </ol>
 *
 * <p>返回确定性排序后的<b>深副本</b>（新 {@link ServingIndex}/{@link ServingIndexField}），不改动入参。
 * 刻意不携带 Mongo 原始元信息（{@code v}/{@code ns}）—— {@link ServingIndex} 本就没有这些字段。</p>
 */
public final class ServingIndexNormalizer {

	private ServingIndexNormalizer() {
	}

	/**
	 * 返回确定性排序 + 方向规范后的深副本；不改动入参。{@code null} 原样返回 {@code null}
	 * （不给无索引的 Module 塞空数组，配合 {@code @JsonInclude(NON_EMPTY)} 保持字段缺省）。
	 */
	public static List<ServingIndex> normalize(List<ServingIndex> indexes) {
		if (indexes == null) {
			return null;
		}
		List<ServingIndex> copy = new ArrayList<>(indexes.size());
		for (ServingIndex idx : indexes) {
			copy.add(canonical(idx));
		}
		copy.sort(Comparator.comparing(ServingIndexNormalizer::signature));
		return copy;
	}

	/** 深复制一条索引，并把每个字段的方向规范为显式布尔。 */
	private static ServingIndex canonical(ServingIndex idx) {
		if (idx == null) {
			return null;
		}
		List<ServingIndexField> fields = null;
		if (idx.getFields() != null) {
			fields = new ArrayList<>(idx.getFields().size());
			for (ServingIndexField f : idx.getFields()) {
				Boolean asc = Boolean.FALSE.equals(f.getAsc()) ? Boolean.FALSE : Boolean.TRUE;
				fields.add(new ServingIndexField(f.getField(), asc));
			}
		}
		return new ServingIndex(idx.getName(), idx.getUnique(), fields);
	}

	/** 「有序字段 + 方向」签名，如 {@code a:1,b:-1}；仅用于确定性定序（不含 name/unique）。 */
	private static String signature(ServingIndex idx) {
		if (idx == null || idx.getFields() == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (ServingIndexField f : idx.getFields()) {
			if (sb.length() > 0) {
				sb.append(',');
			}
			sb.append(f.getField()).append(':').append(Boolean.FALSE.equals(f.getAsc()) ? -1 : 1);
		}
		return sb.toString();
	}
}
