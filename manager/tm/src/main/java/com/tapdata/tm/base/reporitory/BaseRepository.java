package com.tapdata.tm.base.reporitory;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.annotation.SetOnInsert;
import com.tapdata.manager.common.utils.ReflectionUtils;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MapUtils;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MappingMongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 4:33 下午
 * @description
 */
public abstract class BaseRepository<Entity extends BaseEntity, ID> {
    private static MongoRepositoryFactory repositoryFactory;
    protected final MongoTemplate mongoOperations;
    protected final MappingMongoEntityInformation<Entity, ID> entityInformation;
    private static Logger log = LoggerFactory.getLogger(BaseRepository.class);

    @Value("${spring.data.mongodb.cursorBatchSize}")
    protected int cursorBatchSize = 1000;

    private final Class<Entity> entityClass;

    public BaseRepository(Class<Entity> entityClass, MongoTemplate mongoOperations) {
        this.entityInformation = (MappingMongoEntityInformation<Entity, ID>) getEntityInformation(entityClass, mongoOperations);
        this.mongoOperations = mongoOperations;
        this.entityClass = entityClass;
        init();
    }

    protected void createIndex(BsonDocument bson, IndexOptions indexOptions) {
        MongoCollection<Document> collection = mongoOperations.getCollection(entityInformation.getCollectionName());

        ListIndexesIterable<Document> indexes = collection.listIndexes();

        boolean isExist = false;
        end:
        for (Document document : indexes) {
            Map<String, Object> indexKey = (Map<String, Object>) document.get("key");
            List<Map.Entry<String, BsonValue>> entries = new ArrayList<>(bson.entrySet());
            if (indexKey.size() != entries.size()) continue;
            for (int i = 0; i < entries.size(); i++) {
                Map.Entry<String, BsonValue> entry = entries.get(i);
                if (!indexKey.containsKey(entry.getKey()) || entry.getValue().equals(indexKey.get(entry.getKey()))) {
                    break;
                }
                if (i == entries.size() - 1) {
                    isExist = true;
                    break end;
                }
            }
        }

        if (!isExist) {
            collection.createIndex(bson, indexOptions);
        }
    }

    private static <ID, T> MongoEntityInformation<T, ID> getEntityInformation(Class<T> tClass, MongoTemplate mongoOperations) {

        if (repositoryFactory == null)
            repositoryFactory = new MongoRepositoryFactory(mongoOperations);

        return repositoryFactory.getEntityInformation(tClass);
    }

    public void beforeUpdateEntity(Entity entity, UserDetail userDetail) {
        entity.setLastUpdAt(new Date());
        entity.setLastUpdBy(userDetail.getUserId());
    }

    public void beforeCreateEntity(Entity entity, UserDetail userDetail) {
        entity.setCreateAt(new Date());
        entity.setUserId(userDetail.getUserId());
        entity.setCreateUser(userDetail.getUsername());
        beforeUpdateEntity(entity, userDetail);
    }

    public void beforeUpsert(Update update, UserDetail userDetail) {

        if (update.getUpdateObject().get("$set") instanceof Document) {
            Document updates = (Document) update.getUpdateObject().get("$set");
            if (updates.get("createAt") == null) {
                update.setOnInsert("createAt", new Date());
            }
            if (updates.get("createBy") == null) {
                update.setOnInsert("createBy", userDetail.getUserId());
            }
            if (updates.get("customerId") == null) {
                update.setOnInsert("customerId", userDetail.getCustomerId());
            }
            if (updates.get("createUser") == null) {
                update.setOnInsert("createUser", userDetail.getUsername());
            }
            if (updates.get("username") == null) {
                update.setOnInsert("username", userDetail.getUsername());
            }
        }
        update.set("lastUpdAt", new Date());
        update.set("lastUpdBy", userDetail.getUserId());
    }

    /**
     * Set user data isolation
     *
     * @param entity     require, Instance of Entity
     * @param userDetail require, Current operator
     */
    public Entity applyUserDetail(Entity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");
        Assert.notNull(userDetail, "UserDetail must not be null!");

        entity.setCustomId(userDetail.getCustomerId());
        entity.setUserId(userDetail.getUserId());

        return entity;
    }

    /**
     * Set user data isolation
     *
     * @param query      require, Query document criteria
     * @param userDetail require, Current operator
     */
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
            query.addCriteria(Criteria.where("user_id").is(userDetail.getUserId()));
        }
        return query;
    }

    public static void removeFilter(String key, Query query) {
        Field criteriaField = null;
        try {
            criteriaField = Query.class.getDeclaredField("criteria");
            criteriaField.setAccessible(true);
            Map<String, CriteriaDefinition> criteria = (Map<String, CriteriaDefinition>) criteriaField.get(query);
            criteria.remove(key);
        } catch (Exception e) {
            log.error("Remove {} in query {} failed", key, query, e);
        }
    }

    /**
     * set user data to Criteria
     *
     * @param where
     * @param userDetail
     */
    public Criteria applyUserDetail(Criteria where, UserDetail userDetail) {

        Assert.notNull(where, "Criteria must not be null!");
        Assert.notNull(userDetail, "UserDetail must not be null!");

        Document queryCriteria = where.getCriteriaObject();
        if (!queryCriteria.containsKey("customerId")) {
            where.and("customerId").is(userDetail.getCustomerId());
        }

        if (!userDetail.isRoot()) {
            if (!queryCriteria.containsKey("createBy")) {
                where.and("createBy").is(userDetail.getUserId());
            }
        }

        return where;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
     */
    public Entity save(Entity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        if (entityInformation.isNew(entity)) {
            applyUserDetail(entity, userDetail);
            beforeCreateEntity(entity, userDetail);
            return mongoOperations.insert(entity, entityInformation.getCollectionName());
        }
        beforeUpdateEntity(entity, userDetail);

        //return mongoOperations.save(entity, entityInformation.getCollectionName());
        // mongoOperations.updateFirst()
        Query query = getIdQuery(entity.getId());
        applyUserDetail(query, userDetail);

        Update update = buildUpdateSet(entity, userDetail);

        UpdateResult result = mongoOperations.updateFirst(query, update, entityInformation.getJavaType());

        if (result.getMatchedCount() == 1) {
            return findOne(query, userDetail).orElse(entity);
        }
        return entity;
    }

    public Update buildUpdateSet(Entity entity) {
        return buildUpdateSet(entity, null);
    }
    public Update buildUpdateSet(Entity entity, UserDetail userDetail) {
        Update update = new Update();
        Field[] files = ReflectionUtils.getAllDeclaredFields(entityClass);
        if (userDetail != null){
            applyUserDetail(entity, userDetail);
        }
        for (int i = 0; i < files.length; i++) {
            Field field = files[i];
            if ("$jacocoData".equals(field.getName())) {
                continue;
            }
            Object value = ReflectionUtils.getField(field, entity);
            if (value != null) {
                /*org.springframework.data.mongodb.core.mapping.Field fieldDef =
                        field.getAnnotation(org.springframework.data.mongodb.core.mapping.Field.class);*/
                String fieldName = field.getName();
                /*if ( fieldDef != null && StringUtils.hasText(fieldDef.value())) {
                    fieldName = fieldDef.value();
                }*/
                SetOnInsert setOnInsert = field.getAnnotation(SetOnInsert.class);
                if (setOnInsert != null) {
                    update.setOnInsert(fieldName, value);
                } else {
                    update.set(fieldName, value);
                }
            }
        }
        return update;
    }

    protected Update buildReplaceSet(Entity entity) {
        Update update = new Update();
        Field[] files = ReflectionUtils.getAllDeclaredFields(entityClass);
        for (Field field : files) {
            if (field.getName().equals("id")) {
                continue;
            }
            Object value = ReflectionUtils.getField(field, entity);
            update.set(field.getName(), value);
        }
        return update;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.repository.MongoRepository#saveAll(java.lang.Iterable)
     */
    public List<Entity> saveAll(Iterable<Entity> entities, UserDetail userDetail) {
        Assert.notNull(entities, "The given Iterable of entities not be null!");

        Streamable<Entity> source = Streamable.of(entities);

        // source.forEach(entity -> applyUserDetail(entity, userDetail));

        boolean allNew = source.stream().allMatch(entityInformation::isNew);

        if (allNew) {
            List<Entity> result = source.stream().peek(entity -> {
                applyUserDetail(entity, userDetail);
                beforeCreateEntity(entity, userDetail);
            }).collect(Collectors.toList());
            return new ArrayList<>(mongoOperations.insert(result, entityInformation.getCollectionName()));
        }

        return source.stream().map(entity -> save(entity, userDetail)).collect(Collectors.toList());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.io.Serializable)
     */
    public Optional<Entity> findById(ID id, UserDetail userDetail) {
        Assert.notNull(id, "The given id must not be null!");
        Assert.notNull(userDetail, "The given user must not be null!");

        return findOne(Query.query(Criteria.where("_id").is(id)), userDetail);

		/*return Optional.ofNullable(
			mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName()));*/
    }


    public Optional<Entity> findById(String id) {
        Assert.notNull(id, "The given id must not be null!");

        return findOne(Query.query(Criteria.where("_id").is(id)));

		/*return Optional.ofNullable(
			mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName()));*/
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.io.Serializable)
     */
    public Optional<Entity> findById(ID id, com.tapdata.tm.base.dto.Field field, UserDetail userDetail) {
        Assert.notNull(id, "The given id must not be null!");
        Assert.notNull(userDetail, "The given user must not be null!");
        Query query = Query.query(where("_id").is(id));
        applyField(query, field);
        return findOne(query, userDetail);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.io.Serializable)
     */
    public Optional<Entity> findById(ID id, com.tapdata.tm.base.dto.Field field) {
        Assert.notNull(id, "The given id must not be null!");
        Query query = Query.query(where("_id").is(id));
        applyField(query, field);
        return findOne(query);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#existsById(java.lang.Object)
     */
    public boolean existsById(ID id) {
        Assert.notNull(id, "The given id must not be null!");

        return mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
                entityInformation.getCollectionName());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#count()
     */
    public long count(Query query) {
        Assert.notNull(query, "Query must not be null!");

        return mongoOperations.count(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#count()
     */
    public long count(Query query, UserDetail userDetail) {
        Assert.notNull(query, "Query must not be null!");

        applyUserDetail(query, userDetail);

        return mongoOperations.count(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
    }

    /**
     * @param where
     * @return
     */
    public long count(Where where) {
        Criteria criteria = buildCriteria(where, entityInformation);
        return this.count(Query.query(criteria));
    }

    /**
     * @param where
     * @param userDetail
     * @return
     */
    public long count(Where where, UserDetail userDetail) {
        Criteria criteria = buildCriteria(where, entityInformation);
        return this.count(Query.query(criteria), userDetail);
    }

    public UpdateResult updateFirst(Query query, Update update, UserDetail userDetail) {
        Assert.notNull(query, "Query must not be null!");

        Assert.notNull(update, "Update must not be null!");

        applyUserDetail(query, userDetail);
        update.set("lastUpdAt", new Date());
        update.set("lastUpdBy", userDetail.getUserId());

        return mongoOperations.updateFirst(query, update, entityClass);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#deleteById(java.lang.Object)
     */
    public boolean deleteById(ID id, UserDetail userDetail) {
        Assert.notNull(id, "The given id must not be null!");

        Query query = getIdQuery(id);
        applyUserDetail(query, userDetail);

        DeleteResult result = mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
        return result.getDeletedCount() == 1;
    }

    public boolean deleteById(ID id) {
        Assert.notNull(id, "The given id must not be null!");

        Query query = getIdQuery(id);

        DeleteResult result = mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
        return result.getDeletedCount() == 1;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
     */
    public void delete(Entity entity, UserDetail userDetail) {
        Assert.notNull(entity, "The given entity must not be null!");

        Query query = getRemoveByQuery(entity);
        applyUserDetail(query, userDetail);

        DeleteResult deleteResult = mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());

        if (entityInformation.isVersioned() && deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 0) {
            throw new OptimisticLockingFailureException(String.format(
                    "The entity with id %s with version %s in %s cannot be deleted! Was it modified or deleted in the meantime?",
                    entityInformation.getId(entity), entityInformation.getVersion(entity),
                    entityInformation.getCollectionName()));
        }
    }


    public void deleteAll(Iterable<? extends Entity> entities, UserDetail userDetail) {
        Assert.notNull(entities, "The given Iterable of entities not be null!");

        entities.forEach(entity -> delete(entity, userDetail));
    }

    /**
     * Remove all documents from the specified collection that match the provided query document criteria.
     *
     * @param where      required, query document criteria
     * @param userDetail user scope
     */
    public void deleteAll(Where where, UserDetail userDetail) {
        Criteria criteria = buildCriteria(where, entityInformation);
        this.deleteAll(Query.query(criteria), userDetail);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#deleteAll()
     */
    public void deleteAll(Query query, UserDetail userDetail) {

        applyUserDetail(query, userDetail);

        mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#deleteAll()
     */
    public long deleteAll(Query query) {
        DeleteResult remove = mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
        return remove.getDeletedCount();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAll()
     */
    public List<Entity> findAll(UserDetail userDetail) {
        return findAll(new Query(), userDetail);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAll()
     */
    public List<Entity> findAll(Query query, UserDetail userDetail) {
        if (query == null) {
            return Collections.emptyList();
        }

        applyUserDetail(query, userDetail);

        return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAllById(java.lang.Iterable)
     */
    public Iterable<Entity> findAllById(Iterable<ID> ids, UserDetail userDetail) {
        Assert.notNull(ids, "The given Ids of entities not be null!");
        return findAll(new Query(new Criteria(entityInformation.getIdAttribute())
                .in(Streamable.of(ids).stream().collect(StreamUtils.toUnmodifiableList()))), userDetail);
    }

    /**
     * query by Filter
     *
     * @param filter optional, page query parameters
     * @return the List of current page
     */
    public List<Entity> findAll(Filter filter) {

        Query query = filterToQuery(filter);

        return this.findAll(query);
    }

    public Query filterToQuery(Filter filter) {
        if (filter == null)
            filter = new Filter();

        final Query query = new Query().cursorBatchSize(cursorBatchSize);

        if (filter.getLimit() > 0)
            query.limit(filter.getLimit());
        if (filter.getSkip() > 0)
            query.skip(filter.getSkip());

        Criteria criteria = buildCriteria(filter.getWhere(), entityInformation);
        query.addCriteria(criteria);

        applyField(query, filter.getFields());
        applySort(query, filter.getSort());
        return query;
    }

    public Criteria whereToCriteria(Where where) {
        Criteria criteria = null;
        if (where == null)
            where = new Where();


        criteria = buildCriteria(where, entityInformation);
        return criteria;
    }

    /**
     * query by Filter
     *
     * @param filter     optional, page query parameters
     * @param userDetail required, current login user certification
     * @return the List of current page
     */
    public List<Entity> findAll(Filter filter, UserDetail userDetail) {

        if (filter == null)
            filter = new Filter();

        final Query query = new Query().cursorBatchSize(cursorBatchSize);

        if (filter.getLimit() > 0)
            query.limit(filter.getLimit());
        if (filter.getSkip() > 0)
            query.skip(filter.getSkip());

        Criteria criteria = buildCriteria(filter.getWhere(), entityInformation);

        query.addCriteria(criteria);

        applyField(query, filter.getFields());
        applySort(query, filter.getSort());

        return this.findAll(query, userDetail);
    }

    @NotNull
    private Criteria addDeleteOption(Where where) {
        Criteria criteria;
        Object deleted = where.get("is_deleted");
        if (!Objects.isNull(deleted) && !(deleted instanceof HashMap) && ! (Boolean) deleted) {
            where.remove("is_deleted");

            Object orOption = where.get("or");
            // 补充逻辑删除字段 {$or :[{"is_deleted":{$exists: false }},{"is_deleted":false}]}
            if (Objects.isNull(orOption)) {
                where.and("or", MapUtils.packageDelOption());
                criteria = buildCriteria(where, entityInformation);
            } else {
                where.remove("or");

                List<Where> whereList = JSON.parseArray(JSON.toJSONString(orOption), Where.class);
                List<Criteria> list = Lists.newArrayList();
                whereList.forEach(w -> {
                    Criteria temp = buildCriteria(w, entityInformation);
                    list.add(temp);
                });
                Criteria criteriaTwo = new Criteria().orOperator(list);

                Criteria criteriaDel = new Criteria().orOperator(Criteria.where("is_deleted").is(false), Criteria.where("is_deleted").exists(false));
                if (where.isEmpty()) {
                    criteria = new Criteria().andOperator(Lists.newArrayList(criteriaTwo, criteriaDel));
                } else {
                    Criteria criteriaOne = buildCriteria(where, entityInformation);
                    criteria = criteriaOne.andOperator(Lists.newArrayList(criteriaTwo, criteriaDel));
                }
            }


        } else {
            criteria = buildCriteria(where, entityInformation);
        }
        return criteria;
    }

    public List<Entity> findAll(Filter filter,String excludeField, UserDetail userDetail) {

        if (filter == null)
            filter = new Filter();

        final Query query = new Query().cursorBatchSize(cursorBatchSize);

        if (filter.getLimit() > 0)
            query.limit(filter.getLimit());
        if (filter.getSkip() > 0)
            query.skip(filter.getSkip());

        Criteria criteria = buildCriteria(filter.getWhere(), entityInformation);
        query.addCriteria(criteria);
        query.fields().exclude(excludeField);

        applyField(query, filter.getFields());
        applySort(query, filter.getSort());

        return this.findAll(query, userDetail);
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
     */
    public List<Entity> findAll(Sort sort, UserDetail userDetail) {
        Assert.notNull(sort, "Sort must not be null!");

        return findAll(new Query().with(sort), userDetail);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Object)
     */
    public Entity insert(Entity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);

        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Iterable)
     */
    public List<Entity> insert(Iterable<Entity> entities, UserDetail userDetail) {

        Assert.notNull(entities, "The given Iterable of entities not be null!");

        List<Entity> list = Streamable.of(entities).stream()
                .peek(entity -> {
                    applyUserDetail(entity, userDetail);
                    beforeCreateEntity(entity, userDetail);
                })
                .collect(StreamUtils.toUnmodifiableList());

        if (list.isEmpty()) {
            return list;
        }

        return new ArrayList<>(mongoOperations.insertAll(list));
    }

    /**
     * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
     * type.
     *
     * @param query      required, query document criteria
     * @param userDetail scope
     * @return
     */
    public Optional<Entity> findOne(Query query, UserDetail userDetail) {
        Assert.notNull(query, "Query must not be null!");

        applyUserDetail(query, userDetail);

        return Optional.ofNullable(
                mongoOperations.findOne(query, entityInformation.getJavaType(), entityInformation.getCollectionName()));
    }

    public Optional<Entity> findOne(Query query) {
        Assert.notNull(query, "Query must not be null!");

        return Optional.ofNullable(
                mongoOperations.findOne(query, entityInformation.getJavaType(), entityInformation.getCollectionName()));
    }

    /*
     * (non-Javadoc)
     * @see com.tapdata.manager.base.reporitory.BaseRepository#findOne(org.springframework.data.mongodb.core.query.Query, com.tapdata.manager.config.security.UserDetail)
     */
    public Optional<Entity> findOne(Where where, UserDetail userDetail) {
        Assert.notNull(where, "Where must not be null!");

        Criteria criteria = buildCriteria(where, entityInformation);
        return findOne(Query.query(criteria), userDetail);
    }

    public <T> List<T> findDistinct(Query query, String field, UserDetail userDetail, Class<T> resultClass) {

        applyUserDetail(query, userDetail);

        return mongoOperations.findDistinct(query, field, entityInformation.getCollectionName(), resultClass);
    }

    public Query getIdQuery(Object id) {
        return new Query(getIdCriteria(id));
    }

    public Criteria getIdCriteria(Object id) {
        return where(entityInformation.getIdAttribute()).is(id);
    }

    public Query getRemoveByQuery(Entity entity) {
        return isVersionedEntity() ? getQueryForVersion(entity) : getByIdQuery(entity);
    }

    public boolean isVersionedEntity() {
        return entityInformation.isVersioned();
    }

    public Query getQueryForVersion(Entity entity) {
        return new Query(Criteria.where(entityInformation.getIdAttribute()).is(entityInformation.getId(entity))
                .and(entityInformation.getVersionAttribute()).is(entityInformation.getVersion(entity)));
    }

    public Query getByIdQuery(Entity entity) {
        return getIdQuery(entityInformation.getId(entity));
    }

    public String getIdAttribute() {
        return entityInformation.getIdAttribute();
    }

    public long upsert(Query query, Entity entity, UserDetail userDetail) {

        applyUserDetail(query, userDetail);
        Update update = buildUpdateSet(entity, userDetail);

        beforeUpsert(update, userDetail);

        UpdateResult result = mongoOperations.upsert(query, update, entityClass);

        return result.getModifiedCount();
    }

    public long upsert(Query query, Entity entity) {

        Update update = buildUpdateSet(entity);

        UpdateResult result = mongoOperations.upsert(query, update, entityClass);

        return result.getModifiedCount();
    }

    public UpdateResult upsert(Query query, Update update) {
        return mongoOperations.upsert(query, update, entityClass);
    }


    public List<Entity> findAll(Where where, UserDetail userDetail) {

        Query query = new Query();
        query.addCriteria(buildCriteria(where, entityInformation));
        applyUserDetail(query, userDetail);

        return mongoOperations.find(query, entityClass);
    }


    public List<Entity> findAll(Where where) {
        Query query = new Query();
        query.addCriteria(buildCriteria(where, entityInformation));
        return mongoOperations.find(query, entityClass);
    }

    public UpdateResult update(Query query, Update update, UserDetail userDetail) {

        applyUserDetail(query, userDetail);

        return mongoOperations.updateMulti(query, update, entityClass);
    }

    public UpdateResult updateByWhere(Query query, Entity set, Entity setOnInsert, Map<String, Object> unset, UserDetail userDetail) {
        Update update = buildUpdateSet(set, userDetail);
        if (setOnInsert != null) {
            Field[] files = ReflectionUtils.getAllDeclaredFields(entityClass);
            for (int i = 0; i < files.length; i++) {
                Field field = files[i];
                Object value = ReflectionUtils.getField(field, setOnInsert);
                if (value != null) {
                    update.setOnInsert(field.getName(), value);
                }
            }
        }
        if (unset != null) {
            unset.keySet().forEach(key -> {
                Object value = unset.get(key);
                if (value == null) {
                    // skip unset
                } else if (value instanceof Integer && ((Integer) value) == 1) {
                    update.unset(key);
                } else if (value instanceof Boolean && (Boolean) value) {
                    update.unset(key);
                } else if (value instanceof String && "true".equals(value)) {
                    update.unset(key);
                }
            });
        }
        return update(query, update, userDetail);
    }

    public UpdateResult updateByWhere(Query query, Entity entity, UserDetail userDetail) {
        Update update = buildUpdateSet(entity, userDetail);
        return update(query, update, userDetail);
    }

    public List<Entity> findAll(Query query) {
        if (query == null) {
            return Collections.emptyList();
        }
        return mongoOperations.find(query, entityClass);
    }

    public UpdateResult update(Query query, Update update) {
        return mongoOperations.updateFirst(query, update, entityClass);
    }

    public UpdateResult update(Query query,  Entity entity) {
        Update update = buildUpdateSet(entity);
        return mongoOperations.updateFirst(query, update, entityClass);
    }

    public Entity findAndModify(Query query, Update update, UserDetail userDetail) {
        return findAndModify(query, update, null, userDetail);
    }

    public Entity findAndModify(Query query, Update update, FindAndModifyOptions options, UserDetail userDetail) {

        if (options == null) {
            options = new FindAndModifyOptions();
            options.returnNew(true);
        }

        applyUserDetail(query, userDetail);
        beforeUpsert(update, userDetail);

        return findAndModify(query, update, options);
    }

    public Entity findAndModify(Query query, Update update, FindAndModifyOptions options) {
        Entity entity = mongoOperations.findAndModify(query, update, options, entityClass);
        return entity;
    }

    public Entity replaceById(Query query, Entity entity, UserDetail userDetail) {
        Update update = buildReplaceSet(entity);
        mongoOperations.updateFirst(query, update, entityClass);
        return findOne(query, userDetail).orElse(entity);
    }

    public Entity replaceOrInsert(Query query, Entity entity, UserDetail userDetail) {
        Update update = buildReplaceSet(entity);
        mongoOperations.upsert(query, update, entityClass);
        return findOne(query, userDetail).orElse(entity);
    }

    public <T> AggregationResults<T> aggregate(Aggregation aggregation, Class<T> outputType) {


        /*Aggregation aggregation = Aggregation.newAggregation(
                //new MatchOperation(Criteria.where("id").is(toObjectId("5f9400009eb0c95fba755a7b"))),
                Aggregation.match(where("id").is(toObjectId("5f9400009eb0c95fba755a7b"))),
                new ProjectionOperation().andInclude("clusterId"),
                new LookupOperation(
                        Fields.field("mdb_instance"),
                        Fields.field("clusterId"),
                        Fields.field("clusterId"),
                        Fields.field("instances")));*/
        return mongoOperations.aggregate(aggregation, entityClass, outputType);
    }

    protected void init() {

    }

    public MongoTemplate getMongoOperations() {
        return mongoOperations;
    }

    public String getCollectionName() {
        return entityInformation.getCollectionName();
    }

    /**
     * 只查询没有被逻辑删除的
     *
     * @param filter
     * @return
     */
  /*  public Page findPage(JSONObject where, TmPageable tmPageable) {
        Query query = new Query();

        if (null != where) {
            Iterator iter = where.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String proName = entry.getKey().toString();
                String proValue = entry.getValue().toString();
                if (proValue.contains("$in")) {
                    JSONObject jsonObjectValue = JSONUtil.parseObj(proValue);
                    JSONArray jsonArray = jsonObjectValue.getJSONArray("$in");
                    List list = jsonArray.toList(Object.class);
                    query.addCriteria(Criteria.where(proName).in(list));
                } else {
                    query.addCriteria(Criteria.where(proName).is(proValue));
                }
            }
        }
        query.addCriteria(Criteria.where("is_deleted").ne(true));
        Long total = this.mongoOperations.count(query, entityClass);
        //page由limit 和skip计算的来
        List records = this.mongoOperations.find(query.with(tmPageable),entityClass);
        Page<Entity> result = new Page(total, records);
        return result;
    }*/


    /**
     * 只查询没有被逻辑删除的
     *
     * @param where
     * @return
     */
    public List findAll(JSONObject where) {
        Query query = new Query();

        if (null != where) {
            Iterator iter = where.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String proName = entry.getKey().toString();
                String proValue = entry.getValue().toString();
                if (proValue.contains("$in")) {
                    JSONObject jsonObjectValue = JSONUtil.parseObj(proValue);
                    JSONArray jsonArray = jsonObjectValue.getJSONArray("$in");
                    List list = jsonArray.toList(Object.class);
                    query.addCriteria(Criteria.where(proName).in(list));
                } else {
                    query.addCriteria(Criteria.where(proName).is(proValue));
                }
            }
        }
        query.addCriteria(Criteria.where("is_deleted").ne(true));
        //page由limit 和skip计算的来
        List records = this.mongoOperations.find(query,entityClass);
        return records;
    }

    /**
     * 批量操作方法
     * @param mode
     * @return
     */
    public BulkOperations bulkOperations(BulkOperations.BulkMode mode) {
        return mongoOperations.bulkOps(mode, entityClass);
    }



}
