package io.tapdata.mongodb.net.dao;

import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.mongodb.annotation.EnsureMongoDBIndex;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.entity.NodeRegistryEntity;
import io.tapdata.mongodb.entity.ProxySubscriptionEntity;

@MongoDAO(dbName = "proxy")
@EnsureMongoDBIndex(value = "{\"subscription.subscribeIds\" : 1}")
public class ProxySubscriptionDAO extends ToDocumentMongoDAO<ProxySubscriptionEntity> {

}
