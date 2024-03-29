package com.tapdata.tm.events.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.events.entity.Events;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class EventsRepository extends BaseRepository<Events, ObjectId>  {


    public EventsRepository(MongoTemplate mongoOperations) {
        super(Events.class, mongoOperations);
    }

}
