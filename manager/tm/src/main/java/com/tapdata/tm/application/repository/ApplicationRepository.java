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
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class ApplicationRepository extends BaseRepository<ApplicationEntity, ObjectId> {
    private static final java.lang.reflect.Field QUERY_CRITERIA_FIELD = queryCriteriaField();

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
                applyOwnershipFilter(query, new Criteria().orOperator(
                        Criteria.where("user_id").is(userDetail.getUserId()),
                        Criteria.where("user_id").exists(false)));
            }
        }
        return query;
    }

    /**
     * Combine the ownership condition with the caller's root criteria in a single {@code $and}.
     * <p>
     * The ownership condition must neither share the "_id" key with an existing id/_id criterion (the exact id
     * filter would be overwritten when the query is mapped) nor be appended to a caller root criteria that
     * already carries {@code $and}/{@code $or} (Criteria rejects a second {@code $and} key).
     */
    static void applyOwnershipFilter(Query query, Criteria ownership) {
        Map<String, CriteriaDefinition> criteria = criteriaOf(query);
        List<Criteria> combined = new ArrayList<>();
        for (String key : new String[]{null, "$or"}) {
            CriteriaDefinition existing = criteria.get(key);
            if (existing == null) {
                continue;
            }
            if (!(existing instanceof Criteria)) {
                throw new IllegalStateException("Unsupported root criteria for application ownership filter: " + existing);
            }
            criteria.remove(key);
            if (!((Criteria) existing).getCriteriaObject().isEmpty()) {
                combined.add((Criteria) existing);
            }
        }
        if (combined.isEmpty()) {
            query.addCriteria(ownership);
        } else {
            combined.add(ownership);
            query.addCriteria(new Criteria().andOperator(combined));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, CriteriaDefinition> criteriaOf(Query query) {
        try {
            return (Map<String, CriteriaDefinition>) QUERY_CRITERIA_FIELD.get(query);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Read query criteria failed", e);
        }
    }

    private static java.lang.reflect.Field queryCriteriaField() {
        try {
            java.lang.reflect.Field field = Query.class.getDeclaredField("criteria");
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Query criteria field not found", e);
        }
    }

    @Override
    public Criteria applyUserDetail(Criteria where, UserDetail userDetail) {
        return super.applyUserDetail(where, userDetail);
    }
}
