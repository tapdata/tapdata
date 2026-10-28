package com.tapdata.tm.audit;

/** 异步操作最终结果的写入入口，供企业版等模块在对象达到最终状态时调用；没有受理登记时不写事件。 */
public interface AuditOperationFinisher {
	void finish(String objectType, String objectId, String objectName, AuditOutcome outcome, String reason);
}
