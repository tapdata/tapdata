package com.tapdata.tm.module.dto;

/**
 * 服务型索引的「身份签名」。TAP-12057 · P2-2。
 *
 * <p><b>身份 = 有序字段 + 方向（仅此，项目红线）</b>。本类是该身份定义的<b>唯一</b>落点：
 * P2-1 的确定性定序（{@link ServingIndexNormalizer}）与 P2-2 的加载侧比对（「已收录进本/他 API」）
 * 都经此计算签名，避免两处各写一份而对「身份」漂移。</p>
 *
 * <p>形如 {@code a:1,b:-1}。方向语义同 P0：{@code asc==FALSE→-1}，{@code TRUE/null→1}。
 * {@code name}/{@code unique} 绝不进签名（名仅展示、{@code unique} 仅作创建参数）。</p>
 */
public final class ServingIndexSignature {

	private ServingIndexSignature() {
	}

	/** 「有序字段 + 方向」签名；{@code null}/空字段返回空串。 */
	public static String of(ServingIndex index) {
		if (index == null || index.getFields() == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (ServingIndexField f : index.getFields()) {
			if (sb.length() > 0) {
				sb.append(',');
			}
			sb.append(f.getField()).append(':').append(Boolean.FALSE.equals(f.getAsc()) ? -1 : 1);
		}
		return sb.toString();
	}
}
