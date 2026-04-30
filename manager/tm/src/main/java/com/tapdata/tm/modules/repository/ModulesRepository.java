package com.tapdata.tm.modules.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.entity.ModulesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.Optional;

/**
 * @Author:
 * @Date: 2021/10/14
 * @Description:
 */
@Repository
public class ModulesRepository extends BaseRepository<ModulesEntity, ObjectId> {
    public ModulesRepository(MongoTemplate mongoOperations) {
        super(ModulesEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

	public ModulesEntity importEntity(ModulesEntity entity, UserDetail userDetail) {
		Assert.notNull(entity, "Entity must not be null!");

		// 保留导出文件中原始的 userId 和 createUser，避免被当前操作用户覆盖
		String originalUserId = entity.getUserId();
		String originalCreateUser = entity.getCreateUser();

		applyUserDetail(entity, userDetail);
		beforeCreateEntity(entity, userDetail);

		// 恢复导出环境的 userId 和 createUser（与导出数据保持一致）
		if (originalUserId != null && !originalUserId.isEmpty()) {
			entity.setUserId(originalUserId);
		}
		if (originalCreateUser != null && !originalCreateUser.isEmpty()) {
			entity.setCreateUser(originalCreateUser);
		}

		Query id = new Query(Criteria.where("_id").is(entity.getId()));
		upsert(id, entity);
		Optional<ModulesEntity> one = findOne(id, userDetail);
		return one.orElse(null);
	}
}
