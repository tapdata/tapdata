package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.ProxySubscription;

import java.util.Collection;
import java.util.List;

public interface ProxySubscriptionService {
	void syncProxySubscription(ProxySubscription proxySubscription);
	List<String> subscribedNodeIds(String service, String subscribeId);

	List<String> subscribedNodeIds(String service, Collection<String> subscribeIds);

	boolean delete(String id);

	boolean delete(String id, Long time);

	ProxySubscription get(String id);

}
