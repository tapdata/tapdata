package com.tapdata.tm.audit;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/** 批量操作作为一个逻辑操作记录，保留对象级结果（需求 11.4）。 */
@Data
public class AuditBatch {
	private int total;
	private int success;
	private int failed;
	private List<AuditBatchItem> items = new ArrayList<>();

	public void add(AuditBatchItem item) {
		items.add(item);
		total++;
		if (item.getOutcome() == AuditOutcome.FAILED) {
			failed++;
		} else {
			success++;
		}
	}

	public AuditOutcome outcome() {
		if (total == 0 || failed == 0) {
			return AuditOutcome.SUCCESS;
		}
		return success == 0 ? AuditOutcome.FAILED : AuditOutcome.PARTIAL;
	}
}
