package io.tapdata.mongodb.net;

import com.google.common.collect.Lists;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.entity.Subscription;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;
import io.tapdata.mongodb.entity.NodeMessageEntity;
import io.tapdata.mongodb.net.dao.NodeMessageDAO;
import org.bson.Document;
import org.bson.types.ObjectId;

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

	@Override
	public Object getOffsetByTimestamp(Long startTime) {
		if(startTime == null)
			startTime = System.currentTimeMillis();
		NodeMessageEntity nodeMessageEntity = nodeMessageDAO.findOne(new Document("message.time", new Document("$lte", startTime)), "_id");
		if(nodeMessageEntity != null)
			return nodeMessageEntity.getId();
		return null;
	}
	@Override
	public FetchNewDataResult getMessageEntityList(String service, String subscribeId, Object offset, Integer limit) {
		if(limit == null)
			limit = 100;
		MongoCursor<NodeMessageEntity> cursor = null;
		try {
			Document filter = new Document("message.service", service).append("message.subscribeId", subscribeId);
			if(offset != null) {
				filter.append("_id", new Document("$gt", offset));
			}
			FindIterable<NodeMessageEntity> iterable = nodeMessageDAO.getMongoCollection().find(filter).limit(limit);
			cursor = iterable.cursor();
			List<MessageEntity> list = Lists.newArrayList();
			ObjectId lastObjectId = null;
			while (cursor.hasNext()) {
				NodeMessageEntity nodeMessageEntity = cursor.next();
				list.add(nodeMessageEntity.getMessage());
				lastObjectId = (ObjectId) nodeMessageEntity.getId();
			}
			return new FetchNewDataResult().messages(list).offset(lastObjectId);
		} finally {
			if (cursor != null) {
				try {
					cursor.close();
				} catch (Exception e) {
					TapLogger.error(TAG, "cursor close error:{}", e.getMessage());
				}
			}
		}
	}
}
