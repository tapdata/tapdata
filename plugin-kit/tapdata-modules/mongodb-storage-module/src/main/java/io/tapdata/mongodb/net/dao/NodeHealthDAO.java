package io.tapdata.mongodb.net.dao;

import com.mongodb.client.result.UpdateResult;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.entity.NodeHealthMapEntity;
import io.tapdata.mongodb.entity.NodeRegistryEntity;
import org.bson.Document;
import org.bson.conversions.Bson;

import static io.tapdata.mongodb.entity.ToDocument.FIELD_ID;

@MongoDAO(dbName = "proxy")
public class NodeHealthDAO extends ToDocumentMongoDAO<NodeHealthMapEntity> {
	private static final String id = "Nodes_Health";
	private static final String NO_CLEANER = "none";
	private final Bson idFilter = new Document(FIELD_ID, id);

	public NodeHealthMapEntity get() {
		return findOne(new Document(FIELD_ID, id));
	}

	public void addNodeHealth(NodeHealth nodeHealth) {
		updateOne(idFilter, new Document("$set",
				new Document(NodeHealthMapEntity.FIELD_MAP + "." + nodeHealth.getId(),
						new Document("time", nodeHealth.getTime())
								.append("online", nodeHealth.getOnline())
								.append("health", nodeHealth.getHealth())))
				.append("$setOnInsert", new Document(NodeHealthMapEntity.FIELD_CLEANER, NO_CLEANER)), true);
	}

	public boolean assignCleaner(String oldNodeId, String nodeId) {
		if(oldNodeId != null && nodeId != null) {
			NodeHealthMapEntity after = findAndModify(new Document(FIELD_ID, id).append(NodeHealthMapEntity.FIELD_CLEANER, oldNodeId),
					new Document("$set", new Document(NodeHealthMapEntity.FIELD_CLEANER, nodeId)));
			return after != null && nodeId.equals(after.getCleaner());
		}
		return false;
	}

	public String getCleaner() {
		NodeHealthMapEntity nodeHealthMapEntity = findOne(idFilter, NodeHealthMapEntity.FIELD_CLEANER);
		if(nodeHealthMapEntity != null) {
			return nodeHealthMapEntity.getCleaner();
		}
		return null;
	}

	public boolean deleteNodeHealth(String id) {
		UpdateResult updateResult = updateOne(idFilter, new Document("$unset", new Document(NodeHealthMapEntity.FIELD_MAP + "." + id, 1)));
		return updateResult != null && updateResult.getModifiedCount() > 0;
	}

	public NodeHealth getNodeHealth(String id) {
		NodeHealthMapEntity nodeHealthMapEntity = findOne(idFilter, NodeHealthMapEntity.FIELD_MAP + "." + id);
		if(nodeHealthMapEntity != null && nodeHealthMapEntity.getMap() != null) {
			NodeHealth nodeHealth = nodeHealthMapEntity.getMap().get(id);
			if(nodeHealth != null) {
				nodeHealth.setId(id);
				return nodeHealth;
			}
		}
		return null;
	}
}
