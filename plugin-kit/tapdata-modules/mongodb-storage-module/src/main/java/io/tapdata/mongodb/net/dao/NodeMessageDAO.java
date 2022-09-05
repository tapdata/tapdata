package io.tapdata.mongodb.net.dao;

import io.tapdata.mongodb.annotation.EnsureMongoDBIndex;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.entity.NodeMessageEntity;
import io.tapdata.mongodb.entity.NodeRegistryEntity;

@MongoDAO(dbName = "proxy")
@EnsureMongoDBIndex(value = "{\"message.service\" : 1, \"message.subscribeId\" : 1, \"_id\" : 1}")
@EnsureMongoDBIndex(value = "{\"message.time\" : 1}", expireAfterSeconds = 15) //TODO 10 seconds is for test purpose
public class NodeMessageDAO extends ToDocumentMongoDAO<NodeMessageEntity> {

}
