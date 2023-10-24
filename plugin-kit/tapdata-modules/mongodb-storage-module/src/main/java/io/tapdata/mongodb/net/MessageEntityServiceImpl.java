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
import io.tapdata.mongodb.net.dao.NodeMessageV2DAO;
import org.bson.Document;

import java.time.Instant;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

@Implementation(MessageEntityService.class)
public class MessageEntityServiceImpl implements MessageEntityService {

	private static final String TAG = MessageEntityServiceImpl.class.getSimpleName();
	@Bean
	private NodeMessageV2DAO nodeMessageV2DAO;

	@Override
	public void save(MessageEntity message) {
		nodeMessageV2DAO.insertOne(new NodeMessageEntity(message));
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
		nodeMessageV2DAO.getMongoCollection().bulkWrite(writeModels, new BulkWriteOptions().ordered(true));
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
	public String getOffsetByTimestamp(Long startTime) {
		if(startTime == null)
			startTime = System.currentTimeMillis();
		NodeMessageEntity nodeMessageEntity = nodeMessageV2DAO.findOne(new Document("message.time", new Document("$lte", Date.from(Instant.ofEpochMilli(startTime)))), new Document("message.time", -1), "_id");
		if(nodeMessageEntity != null)
			return nodeMessageEntity.getId().toString();
		return null;
	}

	@Override
	public FetchNewDataResult getMessageEntityList(String service, String subscribeId, String offset, Integer limit) {
		return getMessageEntityList(service, subscribeId, offset, limit, null);
	}

	@Override
	public FetchNewDataResult getMessageEntityListDesc(String service, String subscribeId, String offset, Integer limit) {
		return getMessageEntityList(service, subscribeId, offset, limit, map(entry("message.time", -1)));
	}

	@Override
	public FetchNewDataResult getMessageEntityList(String service, String subscribeId, String offset, Integer limit, Map<String, Object> sortMap){
		if(limit == null)
			limit = 100;
		Document sortBson = new Document();
		if (null != sortMap && !sortMap.isEmpty()){
			sortMap.forEach((key, value) -> {
				if (null != key && !"".equals(key.trim()) && null != value)
					sortBson.append(key, value);
			});
		}
		MongoCursor<NodeMessageEntity> cursor = null;
		try {
			Document filter = new Document("message.service", service).append("message.subscribeId", subscribeId);
			if(offset != null) {
				filter.append("_id", new Document("$gt", Long.valueOf(offset)));
			}
			FindIterable<NodeMessageEntity> iterable = nodeMessageV2DAO.getMongoCollection()
					.find(filter);
			if (!sortBson.isEmpty()){
				iterable.sort(sortBson);
			}
			iterable.limit(limit);

			cursor = iterable.cursor();
			List<MessageEntity> list = Lists.newArrayList();
			Long lastObjectId = null;
			while (cursor.hasNext()) {
				NodeMessageEntity nodeMessageEntity = cursor.next();
				list.add(nodeMessageEntity.getMessage());
				lastObjectId = (Long) nodeMessageEntity.getId();
			}
			return new FetchNewDataResult().messages(list).offset(String.valueOf(lastObjectId));
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

	@Override
	public void remove(String service, String subscribeId) {
		nodeMessageV2DAO.delete(new Document("message.service", service).append("message.subscribeId", subscribeId));
	}
}
