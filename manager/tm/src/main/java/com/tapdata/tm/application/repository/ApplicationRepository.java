package com.tapdata.tm.application.repository;

import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.base.IDataPermissionEntity;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.bson.types.ObjectId;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import java.util.ArrayList;
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

    public List<ApplicationEntity> findWithoutClientSecrets(Filter filter, UserDetail userDetail) {
        Query query = filterToQuery(filter);
        applyUserDetail(query, userDetail);
        List<AggregationOperation> pipeline = new ArrayList<>();
        pipeline.add(context -> new Document("$match", context.getMappedObject(query.getQueryObject())));
        if (!query.getSortObject().isEmpty()) {
            pipeline.add(context -> new Document("$sort", context.getMappedObject(query.getSortObject())));
        }
        if (query.getSkip() > 0) {
            pipeline.add(context -> new Document("$skip", query.getSkip()));
        }
        if (query.getLimit() > 0) {
            pipeline.add(context -> new Document("$limit", query.getLimit()));
        }
        pipeline.add(context -> new Document("$set", new Document("computedClientSecretConfigured",
                new Document("$ne", List.of(new Document("$ifNull", List.of("$clientSecret", "")), "")))));
        pipeline.add(context -> new Document("$unset", "clientSecret"));
        Document projection = new Document(query.getFieldsObject());
        boolean inclusion = projection.entrySet().stream().anyMatch(entry -> !entry.getKey().equals("_id")
                && (Boolean.TRUE.equals(entry.getValue()) || entry.getValue() instanceof Number
                && ((Number) entry.getValue()).intValue() != 0));
        boolean idOnly = projection.size() == 1 && (Boolean.TRUE.equals(projection.get("_id"))
                || projection.get("_id") instanceof Number && ((Number) projection.get("_id")).intValue() != 0);
        projection.remove("clientSecret");
        projection.remove("computedClientSecretConfigured");
        if (inclusion || idOnly) {
            projection.put("computedClientSecretConfigured", 1);
        }
        if (!projection.isEmpty()) {
            pipeline.add(context -> new Document("$project", context.getMappedObject(projection)));
        }
        return mongoOperations.aggregate(Aggregation.newAggregation(pipeline), ApplicationEntity.class,
                ApplicationEntity.class).getMappedResults();
    }

    @Override
    protected void init() {
        super.init();
    }
    @Override
    public Query applyUserDetail(Query query, UserDetail userDetail) {
        Assert.notNull(query, "Entity must not be null!");
        Assert.notNull(userDetail, "UserDetail must not be null!");

        removeFilter("customId", query);
        query.addCriteria(Criteria.where("customId").is(userDetail.getCustomerId()));
        if (DataPermissionHelper.setFilterConditions(true, query, userDetail)) {
            return query;
        }
        boolean hasAdminRole = userDetail.getAuthorities().contains(new SimpleGrantedAuthority("ADMIN"));
        if (hasAdminRole) {
            removeFilter("customId", query);
            query.addCriteria(Criteria.where("customId").is(userDetail.getCustomerId()));
        } else {
            removeFilter("customId", query);
            removeFilter("user_id", query);
            query.addCriteria(Criteria.where("customId").is(userDetail.getCustomerId()));
            if ((!userDetail.isRoot()) && !userDetail.isFreeAuth()) {
                // Attach the ownership condition as a top-level $or so it never shares the "_id" key
                // with an existing id/_id criterion; otherwise the exact id filter is overwritten
                // when the query is mapped, or adding the criterion fails outright.
                try {
                    addOrFilter(query,
                            Criteria.where("user_id").is(userDetail.getUserId()),
                            Criteria.where("user_id").exists(false));
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new IllegalStateException("Apply application ownership filter failed", e);
                }
            }
        }
        return query;
    }

    @Override
    public Criteria applyUserDetail(Criteria where, UserDetail userDetail) {
        return super.applyUserDetail(where, userDetail);
    }
}
