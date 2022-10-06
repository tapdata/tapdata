package io.tapdata.mongodb.entity;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.mongodb.error.MongodbErrors;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class MapEntity implements ToDocument {
	private static final String TAG = MapEntity.class.getSimpleName();
	public static final String FIELD_ID = "_id";
	public static final String FIELD_CONTENT_TYPE = "contentType";
	public static final String FIELD_MAP = "map";
	@BsonId
	private String id;
	private String contentType;
	private Document map;
	@BsonIgnore
	private Map<String, TapEntity> entityMap;
	public MapEntity add(String id, TapEntity message) {
		return add(id, message, null);
	}
	public MapEntity add(String id, TapEntity message, String contentType) {
		if(id == null || message == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "id {} or javaCustomSerializer {} is null", id, message);
		}
		this.id = id;
		if(this.contentType == null)
			this.contentType = contentType == null ? message.getClass().getSimpleName() : contentType;
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			message.to(output);
			byte[] data = output.toByteArray();
			map.put(id, data);
		} catch (IOException e) {
			throw new CoreException(NetErrors.JAVA_CUSTOM_DESERIALIZE_FAILED, "Serialize {} failed, {}", this.getClass().getSimpleName(), e.getMessage());
		}
		return this;
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}


	@Override
	public Document toDocument(Document document) {
		if(document != null) {
			document.append(FIELD_ID, id)
					.append(FIELD_CONTENT_TYPE, contentType)
					.append(FIELD_MAP, map);
		}
		return document;
	}
}
