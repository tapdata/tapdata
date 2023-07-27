package com.tapdata.tm.function.python.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.function.python.entity.PythonFunctionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.Optional;

/**
 * @Author: Gavin
 * @Date: 2022/04/07
 * @Description:
 */
@Repository
public class PythonFunctionRepository extends BaseRepository<PythonFunctionEntity, ObjectId> {
    public PythonFunctionRepository(MongoTemplate mongoOperations) {
        super(PythonFunctionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

	public PythonFunctionEntity importEntity(PythonFunctionEntity entity, UserDetail userDetail) {
		Assert.notNull(entity, "Entity must not be null!");

		applyUserDetail(entity, userDetail);
		beforeCreateEntity(entity, userDetail);
		Query id = new Query(Criteria.where("_id").is(entity.getId()));
		upsert(id, entity);
		Optional<PythonFunctionEntity> one = findOne(id, userDetail);
		return one.orElse(null);
	}
}
