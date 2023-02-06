package com.tapdata.tm.application.repository;

import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.List;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class ApplicationRepository extends BaseRepository<ApplicationEntity, ObjectId> {
    public ApplicationRepository(MongoTemplate mongoOperations) {
        super(ApplicationEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
    @Override
    public Query applyUserDetail(Query query, UserDetail userDetail) {
        Assert.notNull(query, "Entity must not be null!");
        Assert.notNull(userDetail, "UserDetail must not be null!");

        boolean hasAdminRole = userDetail.getAuthorities().contains(new SimpleGrantedAuthority("ADMIN"));
        if (hasAdminRole) {
            removeFilter("customId", query);
            query.addCriteria(Criteria.where("customId").is(userDetail.getCustomerId()));
        } else {
            removeFilter("customId", query);
            removeFilter("user_id", query);
            query.addCriteria(Criteria.where("customId").is(userDetail.getCustomerId()));
            if ((!userDetail.isRoot()) && !userDetail.isFreeAuth()) {
                query.addCriteria(Criteria.where("_id").exists(true).orOperator(
                        Criteria.where("user_id").is(userDetail.getUserId()),
                        Criteria.where("user_id").exists(false)
                ));
            }
        }
        return query;
    }

    @Override
    public Criteria applyUserDetail(Criteria where, UserDetail userDetail) {
        return super.applyUserDetail(where, userDetail);
    }
}
