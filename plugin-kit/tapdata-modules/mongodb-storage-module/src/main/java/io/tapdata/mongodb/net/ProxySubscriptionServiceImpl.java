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
		proxySubscriptionDAO.insertOne(new ProxySubscriptionEntity(proxySubscription.getNodeId(), proxySubscription));
	}

	@Override
	public List<String> subscribedNodeIds(String subscribeId) {
		List<ProxySubscriptionEntity> subscriptionEntities = proxySubscriptionDAO.find(new Document(ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".subscribeIds", subscribeId), ProxySubscriptionEntity.FIELD_ID);
		List<String> nodeIds = new ArrayList<>();
		for(ProxySubscriptionEntity entity : subscriptionEntities) {
			nodeIds.add(entity.getId());
		}
		return nodeIds;
	}
}
