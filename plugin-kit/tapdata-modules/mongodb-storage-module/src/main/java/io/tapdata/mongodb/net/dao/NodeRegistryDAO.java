package io.tapdata.mongodb.net.dao;

import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.dao.AbstractMongoDAO;
import io.tapdata.mongodb.entity.IdEntity;
import io.tapdata.mongodb.entity.NodeRegistryEntity;

@MongoDAO(dbName = "proxy")
public class NodeRegistryDAO extends ToDocumentMongoDAO<NodeRegistryEntity> {

}
