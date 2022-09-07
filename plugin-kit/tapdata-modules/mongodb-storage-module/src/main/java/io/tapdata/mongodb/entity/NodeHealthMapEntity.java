package io.tapdata.mongodb.entity;

import io.tapdata.modules.api.net.entity.NodeHealth;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;

import java.util.Map;

public class NodeHealthMapEntity implements ToDocument {
	public static final String FIELD_MAP = "map";
	@BsonId
	private String id;
	private Map<String, NodeHealth> map;

	public String getId() {
		return id;
	}

	@Override
	public Document toDocument(Document document) {
		return document.append(FIELD_ID, id)
				.append(FIELD_MAP, map);
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, NodeHealth> getMap() {
		return map;
	}

	public void setMap(Map<String, NodeHealth> map) {
		this.map = map;
	}
}
