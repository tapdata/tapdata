package com.tapdata.tm.audit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** 服务生命周期事件的服务名称、实例与节点。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditNode {
	private String service;
	private String instance;
	private String host;
	private String ip;
}
