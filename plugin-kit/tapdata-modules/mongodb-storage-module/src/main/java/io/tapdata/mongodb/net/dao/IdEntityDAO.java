package io.tapdata.mongodb.net.dao;

import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.entity.IdEntity;

@MongoDAO(dbName = "proxy")
public class IdEntityDAO extends ToDocumentMongoDAO<IdEntity> {

}
