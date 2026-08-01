package com.tapdata.tm.module.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 一次导入要落地的全部工作项 + 落不了地的记录。TAP-12057 · P3-1。
 *
 * <p>两个桶刻意分开：{@link #getTargets()} 是能干的活（P3-3 逐项 {@code QueryIndexes} → 比对 → 创建），
 * {@link #getUnresolved()} 是干不了的活（P3-5 响亮报错）。<b>不把后者悄悄丢掉</b>——静默跳过就等于
 * 「部署报成功、索引没建」，正是本工单要消灭的那种谎。</p>
 */
@Data
public class ServingIndexLandingWorkList {

	private List<ServingIndexLandingTarget> targets = new ArrayList<>();

	private List<UnresolvedServingIndexTarget> unresolved = new ArrayList<>();

	/** 本次导入是否完全无索引可落（两桶都空）——调用方据此直接收工、不打无谓的日志。 */
	public boolean isEmpty() {
		return targets.isEmpty() && unresolved.isEmpty();
	}

	/** 待创建/待比对的声明总条数（并集口径，未按签名去重）。 */
	public int declaredCount() {
		int count = 0;
		for (ServingIndexLandingTarget target : targets) {
			count += target.getDeclared().size();
		}
		return count;
	}
}
