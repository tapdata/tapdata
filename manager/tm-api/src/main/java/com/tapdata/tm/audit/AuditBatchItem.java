package com.tapdata.tm.audit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditBatchItem {
	private String objectId;
	private String objectName;
	private AuditOutcome outcome;
	private String reason;
}
