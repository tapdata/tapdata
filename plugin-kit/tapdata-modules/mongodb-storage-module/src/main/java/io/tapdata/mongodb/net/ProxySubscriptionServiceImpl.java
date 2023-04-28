package io.tapdata.mongodb.net;

import com.mongodb.client.result.DeleteResult;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.mongodb.entity.ProxySubscriptionEntity;
import io.tapdata.mongodb.net.dao.ProxySubscriptionV2DAO;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.tapdata.mongodb.entity.ToDocument.FIELD_ID;

@Implementation(ProxySubscriptionService.class)
public class ProxySubscriptionServiceImpl implements ProxySubscriptionService {
	@Bean
	private ProxySubscriptionV2DAO proxySubscriptionV2DAO;
	@Override
	public void syncProxySubscription(ProxySubscription proxySubscription) {
//		proxySubscriptionV2DAO.insertOne(new ProxySubscriptionEntity(proxySubscription.getUserId(), proxySubscription));
		proxySubscriptionV2DAO.upsertOne(new Document(FIELD_ID, proxySubscription.getNodeId()), new Document("$set", new Document(ProxySubscriptionEntity.FIELD_SUBSCRIPTION, proxySubscription)));
	}

	@Override
	public List<String> subscribedNodeIds(String service, String subscribeId) {
		return getSubscribedNodeIds(service, new Document().append(ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".subscribeIds", subscribeId));
	}

	@Override
	public List<String> subscribedNodeIds(String service, Collection<String> subscribeIds) {
		return getSubscribedNodeIds(service, new Document().append(ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".subscribeIds", new Document("$all", subscribeIds)));
	}

	private List<String> getSubscribedNodeIds(String service, Document filter) {
		filter.append(ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".service", service);
		List<ProxySubscriptionEntity> subscriptionEntities = proxySubscriptionV2DAO.find(filter, ProxySubscriptionEntity.FIELD_SUBSCRIPTION + ".nodeId");
		List<String> nodeIds = new ArrayList<>();
		for(ProxySubscriptionEntity entity : subscriptionEntities) {
			ProxySubscription proxySubscription = entity.getSubscription();
			if(proxySubscription != null) {
				String nodeId = proxySubscription.getNodeId();
				if(nodeId != null && !nodeIds.contains(nodeId))
					nodeIds.add(proxySubscription.getNodeId());
			}
		}
		return nodeIds;
	}

	@Override
	public boolean delete(String id) {
		return delete(id, null);
	}
	@Override
	public boolean delete(String id, Long time) {
		return proxySubscriptionV2DAO.delete(id, time);
	}

	@Override
	public ProxySubscription get(String id) {
		ProxySubscriptionEntity proxySubscriptionEntity = proxySubscriptionV2DAO.findOne(new Document(FIELD_ID, id));
		if(proxySubscriptionEntity != null)
			return proxySubscriptionEntity.getSubscription();
		return null;
	}

}
