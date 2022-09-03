package io.tapdata.mongodb.entity;

import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.mongodb.error.MongodbErrors;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

public class NodeMessageEntity implements ToDocument {
	private static final String TAG = NodeMessageEntity.class.getSimpleName();
	public static final String FIELD_MESSAGE = "message";
	@BsonId
	private ObjectId id;
	private MessageEntity message;
	public NodeMessageEntity() {
	}
	public NodeMessageEntity(MessageEntity message) {
		if(this.message == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "id {} or message {} is null", id, this.message);
		}
		this.message = this.message;
	}

	@Override
	public Object getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public MessageEntity getMessage() {
		return message;
	}

	public void setMessage(MessageEntity message) {
		this.message = message;
	}

	@Override
	public Document toDocument(Document document) {
		if(document != null) {
			document.append(FIELD_ID, id)
					.append(FIELD_MESSAGE, message);
		}
		return document;
	}
}
