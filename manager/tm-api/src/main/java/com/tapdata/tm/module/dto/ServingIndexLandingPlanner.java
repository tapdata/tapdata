package com.tapdata.tm.module.dto;

import io.tapdata.entity.schema.TapIndex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 落地侧自写索引比对（纯函数）。TAP-12057 · P3-2（方案 §3.4 / <b>ADR-0005</b>）。
 *
 * <p>把「平台声明」与「目标环境已有索引」按 <b>有序字段 + 方向</b>（{@link ServingIndexSignature}）配对，
 * 产出三桶：<b>将创建</b> / <b>将跳过</b> / <b>目标多出</b>。名字与 {@code unique} 均不参与配对
 * ——名仅展示、{@code unique} 仅作创建参数；动作只有「创建 / 跳过」两种，不存在"冲突"类别。</p>
 *
 * <p><b>两条实现红线</b>（§3.4「执行上有两个坑」）：</p>
 * <ul>
 *   <li><b>禁复用引擎 {@code tapIndexEquals}</b>——它在 {@code compareIndexName=true} 时按名短路
 *       （同名即判等、不看字段），与本方案取向相反，会把「异名等价」误判、把「同名异字段」漏判。</li>
 *   <li><b>禁依赖连接器 errorCode 85/86</b>——{@code MongodbConnector.createIndex} 把它们 catch 后 {@code continue}、
 *       调用方收到"成功"（86 还记 info「Index already exists」误导排查）。判定必须由前置 {@code QueryIndexes}
 *       的本比对完成，连接器吞异常只能当第二道幂等兜底。</li>
 * </ul>
 *
 * <p>纯函数、无副作用、可离线 TDD：不触库、不触 Spring。调用方（P3-1 落地 service）负责按
 * {@code (连接, 集合)} 聚合声明、并把该集合上读回的物理索引传进来。</p>
 */
public final class ServingIndexLandingPlanner {

	private ServingIndexLandingPlanner() {
	}

	/**
	 * @param declared 该 {@code (连接, 集合)} 上的平台声明（多 API 共表时为并集）
	 * @param existing 目标环境该集合上读回的全部物理索引（{@code QueryIndexes} 结果）
	 */
	public static ServingIndexLandingPlan plan(List<ServingIndex> declared, List<TapIndex> existing) {
		ServingIndexLandingPlan plan = new ServingIndexLandingPlan();

		Map<String, ServingIndex> existingBySignature = new LinkedHashMap<>();
		if (existing != null) {
			for (TapIndex tapIndex : existing) {
				ServingIndex mapped = ServingIndexMapper.toServingIndex(tapIndex);
				existingBySignature.putIfAbsent(ServingIndexSignature.of(mapped), mapped);
			}
		}

		Map<String, ServingIndex> declaredBySignature = mergeBySignature(declared);

		for (Map.Entry<String, ServingIndex> entry : declaredBySignature.entrySet()) {
			ServingIndex declaration = entry.getValue();
			ServingIndex hit = existingBySignature.get(entry.getKey());
			if (hit != null) {
				plan.getSkip().add(new SkippedServingIndex(declaration, hit, uniqueMismatch(declaration, hit)));
			} else {
				plan.getCreate().add(creation(declaration));
			}
		}

		for (Map.Entry<String, ServingIndex> entry : existingBySignature.entrySet()) {
			if (!declaredBySignature.containsKey(entry.getKey())) {
				plan.getExtra().add(entry.getValue());
			}
		}
		return plan;
	}

	/**
	 * 按身份签名把声明并集去重（多 API 共表，§5.5 D1 副作用处理）：同字段集只留一条——同字段集必得同名，
	 * 建两次是自撞。<b>{@code unique} 取更严者</b>：两份声明同字段集、异 {@code unique} 时取 {@code true}
	 * ——建失败是响亮的 11000，好过静默丢掉约束。
	 *
	 * <p>两类入参在此出局：<b>未勾选项</b>（{@code collected=false}，见 <b>ADR-0011</b> D2——Module 里同时存着
	 * 读回后仅知悉的索引，它们不是声明、不能去建）与<b>无字段的声明</b>（空签名推不出索引）。</p>
	 */
	private static Map<String, ServingIndex> mergeBySignature(List<ServingIndex> declared) {
		Map<String, ServingIndex> merged = new LinkedHashMap<>();
		for (ServingIndex declaration : ServingIndexes.collectedOnly(declared)) {
			String signature = ServingIndexSignature.of(declaration);
			if (signature.isEmpty()) {
				continue;
			}
			ServingIndex kept = merged.get(signature);
			if (kept == null) {
				merged.put(signature, declaration);
			} else if (Boolean.TRUE.equals(declaration.getUnique()) && !Boolean.TRUE.equals(kept.getUnique())) {
				merged.put(signature, stricter(kept));
			}
		}
		return merged;
	}

	/** 保留首条声明的展示名与字段，只把 {@code unique} 提到 {@code true}；不改动入参（纯函数）。 */
	private static ServingIndex stricter(ServingIndex kept) {
		// 同 ServingIndexNormalizer：用 setter 而非全参构造——后者会随字段增减改变元数，漏一个就静默丢。
		ServingIndex copy = new ServingIndex(kept.getName(), Boolean.TRUE, kept.getFields());
		copy.setCollected(kept.getCollected());
		copy.setAttribution(kept.getAttribution());
		copy.setAttributionApi(kept.getAttributionApi());
		return copy;
	}

	/**
	 * {@code unique} 与目标是否不一致 —— <b>仅信息注记</b>，不叫冲突、不影响部署结果（§3.4）：在"只加不删"下
	 * 平台从不 drop 重建去修 {@code unique}。{@code null} 视同 {@code false}（未声明 = 非唯一）。
	 */
	private static boolean uniqueMismatch(ServingIndex declaration, ServingIndex existing) {
		return Boolean.TRUE.equals(declaration.getUnique()) != Boolean.TRUE.equals(existing.getUnique());
	}

	/**
	 * 由声明产出「将创建」的索引规格：名字用 {@link ServingIndexName} 确定性推导（<b>不</b>用声明里的展示名，
	 * 那只是从源环境读回的标签）；有序字段与 {@code unique} 照抄声明。
	 */
	private static ServingIndex creation(ServingIndex declaration) {
		List<ServingIndexField> fields = new ArrayList<>();
		if (declaration.getFields() != null) {
			for (ServingIndexField field : declaration.getFields()) {
				fields.add(new ServingIndexField(field.getField(), field.getAsc()));
			}
		}
		return new ServingIndex(ServingIndexName.of(declaration), declaration.getUnique(), fields);
	}
}
