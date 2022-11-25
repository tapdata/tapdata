/**
 * @title: MessageQueueRepository
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.messagequeue.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.messagequeue.entity.MessageQueue;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.Date;
import java.util.HashMap;

@Repository
public class MessageQueueRepository extends BaseRepository<MessageQueue, ObjectId> {

	public MessageQueueRepository(MongoTemplate mongoOperations) {
		super(MessageQueue.class, mongoOperations);
	}


	public void save(MessageQueue entity) {
		Assert.notNull(entity, "Entity must not be null!");

		if (entityInformation.isNew(entity)) {
			entity.setCreateAt(new Date());
			mongoOperations.insert(entity, entityInformation.getCollectionName());
		}else {
			entity.setLastUpdAt(new Date());
			Query query = getIdQuery(entity.getId());

			Update update = buildUpdateSet(entity);

			update(query, update);
		}
	}
}
