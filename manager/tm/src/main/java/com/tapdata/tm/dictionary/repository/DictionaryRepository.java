package com.tapdata.tm.dictionary.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.dictionary.entity.DictionaryEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/19
 * @Description:
 */
@Repository
public class DictionaryRepository extends BaseRepository<DictionaryEntity, ObjectId> {
    public DictionaryRepository(MongoTemplate mongoOperations) {
        super(DictionaryEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
