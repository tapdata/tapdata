package io.tapdata.mongodb.net.dao;

import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.entity.NodeMessageEntity;
import io.tapdata.mongodb.entity.NodeRegistryEntity;

@MongoDAO(dbName = "proxy")
public class NodeMessageDAO extends ToDocumentMongoDAO<NodeMessageEntity> {

}
