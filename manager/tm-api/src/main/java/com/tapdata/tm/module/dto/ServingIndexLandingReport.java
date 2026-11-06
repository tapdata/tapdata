package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一次落地的 dry-run 分桶报告。TAP-12057 · P3-4。
 */
@Data
public class ServingIndexLandingReport implements Serializable {

	private static final long serialVersionUID = 1L;

	private List<ServingIndexTargetReport> targets = new ArrayList<>();

	/** 有声明却落不了地的（连接未命中 / 无表名）——报告里必须显示，静默丢弃等于报成功。 */
	private List<UnresolvedServingIndexTarget> unresolved = new ArrayList<>();

	/** P3-5 汇总：建了多少 / 谁没建成 / 谁落不了地，一处说清；看的人不必自己把三桶加起来数。 */
	private ServingIndexLandingSummary summary = new ServingIndexLandingSummary();

	/** 是否存在需要人干预的情况：任一 target 报错、超 64 上限预检、或有落不了地的声明。 */
	public boolean needsAttention() {
		if (!unresolved.isEmpty()) {
			return true;
		}
		for (ServingIndexTargetReport target : targets) {
			if (target.getError() != null
					|| target.getLevel() == ServingIndexTargetReport.IndexCountLevel.EXCEEDS_LIMIT) {
				return true;
			}
		}
		return false;
	}
}
