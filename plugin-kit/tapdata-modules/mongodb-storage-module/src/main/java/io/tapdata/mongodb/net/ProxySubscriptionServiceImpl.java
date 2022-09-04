package io.tapdata.mongodb.net;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.service.NodeRegistryService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.mongodb.entity.ProxySubscriptionEntity;
import io.tapdata.mongodb.net.dao.ProxySubscriptionDAO;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

@Implementation(ProxySubscriptionService.class)
public class ProxySubscriptionServiceImpl implements ProxySubscriptionService {
	@Bean
	private ProxySubscriptionDAO proxySubscriptionDAO;
	@Override
	public void syncProxySubscription(ProxySubscription proxySubscription) {
		proxySubscriptionDAO.insertOne(new ProxySubscriptionEntity(proxySubscription));
	}

	@Override
	public List<String> subscribedNodeIds(String service, String subscribeId) {
		List<ProxySubscriptionEntity> subscriptionEntities = proxySubscriptionDAO.find(new Document(ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".service", service).append(ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".subscribeIds", subscribeId), ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".nodeId");
		List<String> nodeIds = new ArrayList<>();
		for(ProxySubscriptionEntity entity : subscriptionEntities) {
			ProxySubscription proxySubscription = entity.getSubscription();
			if(proxySubscription != null && proxySubscription.getNodeId() != null)
				nodeIds.add(proxySubscription.getNodeId());
		}
		return nodeIds;
	}
}
