package com.tapdata.tm.audit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** 单个字段的变更；敏感字段 before/after 为空，只在 secretState 中记录设置、替换、清除或未变更。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditChange {
	private String field;
	private String before;
	private String after;
	private String secretState;

	public static AuditChange secret(String field, String state) {
		return new AuditChange(field, null, null, state);
	}
}
