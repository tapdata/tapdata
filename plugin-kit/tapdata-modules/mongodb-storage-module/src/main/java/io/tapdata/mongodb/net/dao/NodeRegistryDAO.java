package io.tapdata.mongodb.net.dao;

import com.mongodb.client.result.DeleteResult;
import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.entity.NodeRegistryEntity;
import org.bson.Document;

import static io.tapdata.mongodb.entity.ToDocument.FIELD_ID;

@MongoDAO(dbName = "proxy")
public class NodeRegistryDAO extends ToDocumentMongoDAO<NodeRegistryEntity> {
	public boolean delete(String id, Long time) {
		if(id == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "Delete node registry failed, missing id");
		Document filter = new Document(FIELD_ID, id);
		if(time != null) {
			filter.append(NodeRegistryEntity.FIELD_NODE + ".time", time);
		}
		DeleteResult deleteResult = delete(filter);
		return deleteResult != null && deleteResult.getDeletedCount() > 0;
	}
}
