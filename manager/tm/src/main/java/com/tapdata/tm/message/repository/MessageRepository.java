package com.tapdata.tm.message.repository;


import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.message.entity.MessageEntity;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class MessageRepository extends BaseRepository<MessageEntity, ObjectId> {
    public MessageRepository(MongoTemplate mongoOperations) {
        super(MessageEntity.class,mongoOperations);
    }
}
