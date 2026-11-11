package com.tapdata.tm.audit.query;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 逻辑操作：同步操作一个事件；异步操作把受理事件与 SYSTEM 最终事件按 Operation ID 聚合，
 * 列表展示原始发起人与最终结果，详情展示完整时间线（需求 13.3）。
 */
@Data
public class AuditOperationVo {
	private String operationId;
	private AuditEventVo initiator;
	private String outcome;
	private Date timestamp;
	private Date finalTimestamp;
	private boolean pending;
	private List<AuditEventVo> events = new ArrayList<>();
}
