package io.tapdata.mongodb.entity;

import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.mongodb.error.MongodbErrors;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

public class ProxySubscriptionEntity implements ToDocument {
	private static final String TAG = ProxySubscriptionEntity.class.getSimpleName();
	public static final String FIELD_SUBSCRIPTION = "subscription";
	@BsonId
	private ObjectId id;
	private ProxySubscription subscription;
	public ProxySubscriptionEntity() {
	}
	public ProxySubscriptionEntity(ProxySubscription subscription) {
		if(subscription == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "ProxySubscription is null");
		}
		this.id = new ObjectId();
		this.subscription = subscription;
	}

	@Override
	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public ProxySubscription getSubscription() {
		return subscription;
	}

	public void setSubscription(ProxySubscription subscription) {
		this.subscription = subscription;
	}

	@Override
	public Document toDocument(Document document) {
		if(document != null) {
			document.append(FIELD_ID, id)
					.append(FIELD_SUBSCRIPTION, subscription);
		}
		return document;
	}
}
