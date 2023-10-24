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
	public static final String FIELD_SUBSCRIPTION_NODE_ID = "nodeId";
	@BsonId
	private String id;
	public ProxySubscriptionEntity id(String id) {
		this.id = id;
		return this;
	}

	private ProxySubscription subscription;
	public ProxySubscriptionEntity subscription(ProxySubscription subscription) {
		this.subscription = subscription;
		return this;
	}

	public ProxySubscriptionEntity() {
	}
	public ProxySubscriptionEntity(String id, ProxySubscription subscription) {
		if(subscription == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "ProxySubscription is null");
		}
		this.id = id;
		this.subscription = subscription;
	}

	@Override
	public String getId() {
		return id;
	}

	public void setId(String id) {
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
