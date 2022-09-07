package io.tapdata.modules.api.net.entity;

public class Subscription {
	private String service;
	public Subscription service(String service) {
		this.service = service;
		return this;
	}
	private String subscribeId;
	public Subscription subscribeId(String subscribeId) {
		this.subscribeId = subscribeId;
		return this;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getSubscribeId() {
		return subscribeId;
	}

	public void setSubscribeId(String subscribeId) {
		this.subscribeId = subscribeId;
	}
}
