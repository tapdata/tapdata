package com.tapdata.tm.module.dto;

import io.tapdata.entity.schema.TapIndex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 加载默认勾选策略。TAP-12057 · P2-2（方案 §3.8.1 / §3.8.3）。
 *
 * <p>把读回的 {@code List<TapIndex>} 逐条规划成 {@link LoadedServingIndex}：<b>全表可见、不预过滤</b>
 * （§3.8.1，一条不少），归因只影响「可勾/默认勾」。默认勾选<b>自上而下、首个命中即定</b>（§3.8.3）：
 * {@code _id_} → text/wildcard 超范围 → 匹配本 API → 上次已收录本 API → 已被别 API 收录 → 其余。</p>
 *
 * <p>纯函数、无副作用、可离线 TDD：调用方（{@code ServingIndexLoadService}）负责用 {@link ServingIndexSignature}
 * 预先算好「本 API 已收录签名集」与「他 API 签名→API 名」两个上下文再传入；本类不触库、不触 Spring。</p>
 */
public final class ServingIndexLoadPlanner {

	private ServingIndexLoadPlanner() {
	}

	/**
	 * @param indexes            读回的全部物理索引（保序输出、一条不少）
	 * @param pattern            本 API 的等值查询判据（{@link ApiQueryPattern}）
	 * @param thisApiSignatures  本 Module 已收录的索引身份签名集（{@link ServingIndexSignature}）
	 * @param otherApiSignatures 同 (连接,集合) 下他 Module 的「签名 → 来源 API 名」
	 */
	public static List<LoadedServingIndex> plan(List<TapIndex> indexes, ApiQueryPattern pattern,
												Set<String> thisApiSignatures,
												Map<String, String> otherApiSignatures) {
		List<LoadedServingIndex> out = new ArrayList<>();
		if (indexes == null) {
			return out;
		}
		ApiQueryPattern p = pattern != null ? pattern : ApiQueryPattern.from(null);
		Set<String> thisApi = thisApiSignatures != null ? thisApiSignatures : Collections.emptySet();
		Map<String, String> otherApi = otherApiSignatures != null ? otherApiSignatures : Collections.emptyMap();
		for (TapIndex tapIndex : indexes) {
			out.add(classify(tapIndex, p, thisApi, otherApi));
		}
		return out;
	}

	private static LoadedServingIndex classify(TapIndex tapIndex, ApiQueryPattern pattern,
											   Set<String> thisApi, Map<String, String> otherApi) {
		ServingIndex mapped = ServingIndexMapper.toServingIndex(tapIndex);
		String signature = ServingIndexSignature.of(mapped);

		// 首个命中即定（§3.8.3），顺序不可乱。
		if (isIdIndex(mapped)) {
			return verdict(mapped, LoadedIndexAttribution.SYSTEM_INDEX, false, false, null);
		}
		if (!ServingIndexMapper.isSupported(tapIndex)) {
			return verdict(mapped, LoadedIndexAttribution.UNSUPPORTED, false, false, null);
		}
		if (pattern.matches(mapped)) {
			return verdict(mapped, LoadedIndexAttribution.MATCHES_API, true, true, null);
		}
		if (thisApi.contains(signature)) {
			return verdict(mapped, LoadedIndexAttribution.COLLECTED_BY_THIS_API, true, true, null);
		}
		if (otherApi.containsKey(signature)) {
			return verdict(mapped, LoadedIndexAttribution.COLLECTED_BY_OTHER_API, true, false, otherApi.get(signature));
		}
		return verdict(mapped, LoadedIndexAttribution.UNCLASSIFIED, true, false, null);
	}

	/** {@code _id_} 默认索引；口径与落地报告共用一份（{@link ServingIndexes#isDefaultIdIndex}）。 */
	private static boolean isIdIndex(ServingIndex mapped) {
		return ServingIndexes.isDefaultIdIndex(mapped);
	}

	private static LoadedServingIndex verdict(ServingIndex index, LoadedIndexAttribution attribution,
											  boolean checkable, boolean defaultChecked, String attributionApi) {
		return LoadedServingIndex.builder()
				.index(index)
				.attribution(attribution)
				.checkable(checkable)
				.defaultChecked(defaultChecked)
				.attributionApi(attributionApi)
				.build();
	}
}
