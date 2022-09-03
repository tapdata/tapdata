package io.tapdata.mongodb.net;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.mongodb.net.dao.NodeMessageDAO;

import java.util.List;

@Implementation(MessageEntityService.class)
public class MessageEntityServiceImpl implements MessageEntityService {

	@Bean
	private NodeMessageDAO nodeMessageDAO;

	@Override
	public void save(List<MessageEntity> messages) {
//		nodeMessageDAO.getMongoCollection().bulkWrite();
//
//		List<WriteModel<Document>> writeModels = new ArrayList<>();
//		for (MessageEntity message : messages) {
//			nodeMessageDAO.insertOne(new NodeMessageEntity(message));
//			writeModels.add(new UpdateManyModel(query, update.getUpdateObject(), new UpdateOptions().upsert(true)));
//		}
//		clientMongoOperator.executeBulkWrite(writeModels, new BulkWriteOptions().ordered(false), collectionName, Job.ERROR_RETRY, Job.ERROR_RETRY_INTERVAL, job);
	}
}
