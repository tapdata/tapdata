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

public class IdEntity implements ToDocument {
	private static final String TAG = IdEntity.class.getSimpleName();
	@BsonId
	private String id;
	private String contentType;
	private byte[] data;
	@BsonIgnore
	private TapEntity message;
	public IdEntity() {
	}
	public IdEntity(String id, TapEntity message) {
		this(id, message, null);
	}
	public IdEntity(String id, TapEntity message, String contentType) {
		if(id == null || message == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "id {} or javaCustomSerializer {} is null", id, message);
		}
		this.id = id;
		this.message = message;
		this.contentType = contentType == null ? message.getClass().getSimpleName() : contentType;
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			message.to(output);
			data = output.toByteArray();
		} catch (IOException e) {
			throw new CoreException(NetErrors.JAVA_CUSTOM_DESERIALIZE_FAILED, "Serialize {} failed, {}", this.getClass().getSimpleName(), e.getMessage());
		}
	}
	
	public TapEntity getMessage() {
		if(contentType == null || data == null)
			return null;
		if(message == null) {
			synchronized (this) {
				if(message == null) {
					message = ClassFactory.create(TapEntity.class, contentType);
					if(message != null) {
						try(ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
							message.from(bais);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					} else {
						TapLogger.warn(TAG, "getMessage Content type {} doesn't match any TapMessage for {}", contentType, this.getClass().getSimpleName());
					}
				}
			}
		}
		return message;
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

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public void setMessage(TapEntity message) {
		this.message = message;
	}

	@Override
	public Document toDocument(Document document) {
		if(document != null) {
			document.append("_id", id)
					.append("contentType", contentType)
					.append("data", data);
		}
		return document;
	}
}
