package io.tapdata.mongodb.entity;

import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.mongodb.error.MongodbErrors;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;

public class ProxySubscriptionEntity implements ToDocument {
	private static final String TAG = ProxySubscriptionEntity.class.getSimpleName();
	public static final String FIELD_SUBSCRIPTION = "subscription";
	@BsonId
	private String id;
	private ProxySubscription subscription;
	public ProxySubscriptionEntity() {
	}
	public ProxySubscriptionEntity(String id, ProxySubscription subscription) {
		if(id == null || subscription == null) {
			throw new CoreException(MongodbErrors.ILLEGAL_ARGUMENTS, "id {} or ProxySubscription {} is null", id, subscription);
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


	@Override
	public Document toDocument(Document document) {
		if(document != null) {
			document.append(FIELD_ID, id)
					.append(FIELD_SUBSCRIPTION, subscription);
		}
		return document;
	}
}
