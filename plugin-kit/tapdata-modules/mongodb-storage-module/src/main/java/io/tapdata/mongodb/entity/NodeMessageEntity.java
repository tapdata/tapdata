package io.tapdata.mongodb.entity;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.mongodb.error.MongodbErrors;
import io.tapdata.pdk.apis.utils.OrderedIdGenerator;
import org.bson.Document;

public class NodeMessageEntity implements ToDocument {
	private static final String TAG = NodeMessageEntity.class.getSimpleName();
	public static final String FIELD_MESSAGE = "message";
	private Long _id;
	private MessageEntity message;

	private static OrderedIdGenerator orderedIdGenerator;
	static {
		orderedIdGenerator = InstanceFactory.instance(OrderedIdGenerator.class);
	}
	public NodeMessageEntity() {
	}

	public NodeMessageEntity(MessageEntity message) {
		_id = orderedIdGenerator.nextId();
		if(message == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "message is null");
		}
		this.message = message;
	}

	@Override
	public Object getId() {
		return _id;
	}

	public void setId(Long id) {
		this._id = id;
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
			document.append(FIELD_ID, _id)
					.append(FIELD_MESSAGE, message);
		}
		return document;
	}
}
