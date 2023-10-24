package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.ProxySubscription;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ProxySubscriptionService {
	void syncProxySubscription(ProxySubscription proxySubscription);
	List<String> subscribedNodeIdsByAll(String service, String subscribeId);

	List<String> subscribedNodeIdsByAll(String service, Set<String> subscribeIds);

	List<String> subscribedNodeIdsByAnyOne(String service, Collection<String> subscribeIds);

	List<String> subscribedNodeIdsByAll(String service, Set<String> subscribeIds, List<Set<String>> orSubscribeIdList);

	boolean delete(String id);

	boolean delete(String id, Long time);

	ProxySubscription get(String id);

}
