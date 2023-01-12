package com.tapdata.tm.typemappings.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.typemappings.entity.DataTypeSupportEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午2:18
 */
@Repository
public class DataTypeRepository extends BaseRepository<DataTypeSupportEntity, ObjectId> {
    public DataTypeRepository(MongoTemplate mongoOperations) {
        super(DataTypeSupportEntity.class, mongoOperations);
    }
}
