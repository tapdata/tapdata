package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.ProxySubscription;

import java.util.List;

public interface ProxySubscriptionService {
	void syncProxySubscription(ProxySubscription proxySubscription);
	List<String> subscribedNodeIds(String subscribeId);
}
