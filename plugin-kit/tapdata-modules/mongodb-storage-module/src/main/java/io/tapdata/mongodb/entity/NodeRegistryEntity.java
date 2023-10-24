package io.tapdata.mongodb.entity;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.mongodb.error.MongodbErrors;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class NodeRegistryEntity implements ToDocument {
	private static final String TAG = NodeRegistryEntity.class.getSimpleName();
	public static final String FIELD_NODE = "node";
	@BsonId
	private String id;
	private NodeRegistry node;
	public NodeRegistryEntity() {
	}
	public NodeRegistryEntity(String id, NodeRegistry node) {
		if(id == null || node == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "id {} or NodeRegistry {} is null", id, node);
		}
		this.id = id;
		this.node = node;
	}

	@Override
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public NodeRegistry getNode() {
		return node;
	}

	public void setNode(NodeRegistry node) {
		this.node = node;
	}

	@Override
	public Document toDocument(Document document) {
		if(document != null) {
			document.append(FIELD_ID, id)
					.append(FIELD_NODE, node);
		}
		return document;
	}
}
