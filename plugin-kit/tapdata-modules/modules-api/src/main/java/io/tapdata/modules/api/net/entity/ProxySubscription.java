package io.tapdata.modules.api.net.entity;

import java.util.List;
import java.util.Set;

public class ProxySubscription {
	private String nodeId;
	public ProxySubscription nodeId(String nodeId) {
		this.nodeId = nodeId;
		return this;
	}
	private String service;
	public ProxySubscription service(String service) {
		this.service = service;
		return this;
	}
	private Set<String> subscribeIds;
	public ProxySubscription subscribeIds(Set<String> subscribeIds) {
		this.subscribeIds = subscribeIds;
		return this;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public Set<String> getSubscribeIds() {
		return subscribeIds;
	}

	public void setSubscribeIds(Set<String> subscribeIds) {
		this.subscribeIds = subscribeIds;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}
}
