package com.tapdata.tm.function.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.function.entity.JsFunctionEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.Optional;

/**
 * @Author:
 * @Date: 2022/04/07
 * @Description:
 */
@Repository
public class JsFunctionRepository extends BaseRepository<JsFunctionEntity, ObjectId> {
    public JsFunctionRepository(MongoTemplate mongoOperations) {
        super(JsFunctionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

	public JsFunctionEntity importEntity(JsFunctionEntity entity, UserDetail userDetail) {
		Assert.notNull(entity, "Entity must not be null!");

		applyUserDetail(entity, userDetail);
		beforeCreateEntity(entity, userDetail);
		Query id = new Query(Criteria.where("_id").is(entity.getId()));
		upsert(id, entity);
		Optional<JsFunctionEntity> one = findOne(id, userDetail);
		return one.orElse(null);
	}
}
