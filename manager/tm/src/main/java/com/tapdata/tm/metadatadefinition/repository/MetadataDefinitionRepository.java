package com.tapdata.tm.metadatadefinition.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class MetadataDefinitionRepository extends BaseRepository<MetadataDefinitionEntity, ObjectId> {
    public MetadataDefinitionRepository(MongoTemplate mongoOperations) {
        super(MetadataDefinitionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public Query applyUserDetail(Query query, UserDetail userDetail) {
        Assert.notNull(query, "Entity must not be null!");
        Assert.notNull(userDetail, "UserDetail must not be null!");


        removeFilter("customId", query);
        removeFilter("user_id", query);
//        Criteria criteria = new Criteria();
//        criteria.orOperator(Criteria.where("customId").is(userDetail.getCustomerId()), Criteria.where("customId").exists(true));
//        Criteria criteria1 = new Criteria();
//        criteria1.orOperator(Criteria.where("user_id").is(userDetail.getUserId()), Criteria.where("user_id").exists(true));
        query.addCriteria(Criteria.where("customId").is(userDetail.getCustomerId()));
        query.addCriteria(Criteria.where("user_id").is(userDetail.getUserId()));
        return query;
    }

}
