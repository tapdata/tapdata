package io.tapdata.mongodb.net;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.entity.Subscription;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.mongodb.entity.NodeMessageEntity;
import io.tapdata.mongodb.net.dao.NodeMessageDAO;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@Implementation(MessageEntityService.class)
public class MessageEntityServiceImpl implements MessageEntityService {

	private static final String TAG = MessageEntityServiceImpl.class.getSimpleName();
	@Bean
	private NodeMessageDAO nodeMessageDAO;

	@Override
	public void save(MessageEntity message) {
		nodeMessageDAO.insertOne(new NodeMessageEntity(message));
	}

	@Override
	public void save(List<MessageEntity> messages, ChangedSubscribeIdsListener listener) {
		List<WriteModel<NodeMessageEntity>> writeModels = new ArrayList<>();
		Set<String> changedSet = new HashSet<>();
		for (MessageEntity message : messages) {
			String service = message.getService();
			String subscribeId = message.getSubscribeId();
			if(service != null && subscribeId != null) {
				changedSet.add(service + "$" + subscribeId);
				writeModels.add(new InsertOneModel<>(new NodeMessageEntity(message)));
			} else {
				TapLogger.error(TAG, "Message missing service {} or subscribeId {}, message {}", service, subscribeId, toJson(message));
			}
		}
		nodeMessageDAO.getMongoCollection().bulkWrite(writeModels, new BulkWriteOptions().ordered(true));
		if(listener != null) {
			List<Subscription> subscriptions = new ArrayList<>();
			for(String changed : changedSet) {
				int pos = changed.indexOf("$");
				subscriptions.add(new Subscription().service(changed.substring(0, pos)).subscribeId(changed.substring(pos + 1)));
			}
			listener.changed(subscriptions);
		}
	}
}
