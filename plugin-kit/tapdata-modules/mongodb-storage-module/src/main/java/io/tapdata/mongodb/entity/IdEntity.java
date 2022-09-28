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
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class IdEntity implements ToDocument {
	private static final String TAG = IdEntity.class.getSimpleName();
	@BsonId
	private ObjectId id;
	private String contentType;
	private String connectionId;
	public static final int ENCODE_BINARY = 1;
	public static final int ENCODE_BINARY_GZIP = 10;
	private Integer encode;
	private byte[] data;
	@BsonIgnore
	private TapEntity message;
	public IdEntity() {
	}
	public IdEntity(TapEntity message) {
		this(message, null, null);
	}

	public IdEntity(TapEntity message, Integer encode) {
		this(message, null, encode);
	}
	public IdEntity(TapEntity message, String contentType, Integer encode) {
		if(id == null || message == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "id {} or javaCustomSerializer {} is null", id, message);
		}
		this.id = id;
		this.message = message;
		this.contentType = contentType == null ? message.getClass().getSimpleName() : contentType;
		this.encode = encode == null ? ENCODE_BINARY : encode;

		try(ByteArrayOutputStream output = new ByteArrayOutputStream()) {
			if(encode == ENCODE_BINARY_GZIP) {
				try(GZIPOutputStream gzipOutputStream = new GZIPOutputStream(output)) {
					message.to(gzipOutputStream);
				}
			} else {
				message.to(output);
			}
			data = output.toByteArray();
		} catch (IOException e) {
			throw new CoreException(NetErrors.JAVA_CUSTOM_DESERIALIZE_FAILED, "Serialize {} failed, {}", this.getClass().getSimpleName(), e.getMessage());
		}
	}
	
	public TapEntity getEntity() {
		if(contentType == null || data == null)
			return null;
		if(message == null) {
			synchronized (this) {
				if(message == null) {
					message = ClassFactory.create(TapEntity.class, contentType);
					if(message != null) {
						try(ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
							if(encode == ENCODE_BINARY_GZIP) {
								try(GZIPInputStream gzipInputStream = new GZIPInputStream(bais)) {
									message.from(gzipInputStream);
								}
							} else {
								message.from(bais);
							}
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
	

	@Override
	public Object getId() {
		return id;
	}

	public void setId(ObjectId id) {
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

	public TapEntity getMessage() {
		return message;
	}
	public Integer getEncode() {
		return encode;
	}

	public void setEncode(Integer encode) {
		this.encode = encode;
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
