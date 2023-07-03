package com.tapdata.tm.metadatainstance.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.utils.GZIPUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * @Author:
 * @Date: 2021/09/11
 * @Description:
 */
@Repository
public class MetadataInstancesRepository extends BaseRepository<MetadataInstancesEntity, ObjectId> {



    @Value("${compression.fields.length:5}")
    private int compressionFieldsLength;
    public MetadataInstancesRepository(MongoTemplate mongoOperations) {
        super(MetadataInstancesEntity.class, mongoOperations);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
     */
    public MetadataInstancesEntity save(MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        return super.save(entity, userDetail);
    }
    public Update buildUpdateSet(MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        return super.buildUpdateSet(entity, userDetail);
    }

    protected Update buildReplaceSet(MetadataInstancesEntity entity) {
        gzipFields(entity);
        return super.buildReplaceSet(entity);
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.io.Serializable)
     */
    public Optional<MetadataInstancesEntity> findById(ObjectId id, UserDetail userDetail) {
        Optional<MetadataInstancesEntity> optional = super.findById(id, userDetail);
        optional.ifPresent(this::unzipFields);
        return optional;
    }


    public Optional<MetadataInstancesEntity> findById(String id) {

        Optional<MetadataInstancesEntity> optional = super.findById(id);
        optional.ifPresent(this::unzipFields);
        return optional;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.io.Serializable)
     */
    public Optional<MetadataInstancesEntity> findById(ObjectId id, com.tapdata.tm.base.dto.Field field, UserDetail userDetail) {

        Optional<MetadataInstancesEntity> optional = super.findById(id, field, userDetail);
        optional.ifPresent(this::unzipFields);
        return optional;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.io.Serializable)
     */
    public Optional<MetadataInstancesEntity> findById(ObjectId id, com.tapdata.tm.base.dto.Field field) {
        Optional<MetadataInstancesEntity> optional = super.findById(id, field);
        optional.ifPresent(this::unzipFields);
        return optional;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAll()
     */
    public List<MetadataInstancesEntity> findAll(UserDetail userDetail) {
        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(userDetail);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;

    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAll()
     */
    public List<MetadataInstancesEntity> findAll(Query query, UserDetail userDetail) {
        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(query, userDetail);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAllById(java.lang.Iterable)
     */
    public Iterable<MetadataInstancesEntity> findAllById(Iterable<ObjectId> ids, UserDetail userDetail) {

        Iterable<MetadataInstancesEntity> iterable = super.findAllById(ids, userDetail);
        if (iterable != null) {
            for (MetadataInstancesEntity entity : iterable) {
                unzipFields(entity);
            }
        }

        return iterable;
    }

    /**
     * query by Filter
     *
     * @param filter optional, page query parameters
     * @return the List of current page
     */
    public List<MetadataInstancesEntity> findAll(Filter filter) {
        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(filter);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }

    /**
     * query by Filter
     *
     * @param filter     optional, page query parameters
     * @param userDetail required, current login user certification
     * @return the List of current page
     */
    public List<MetadataInstancesEntity> findAll(Filter filter, UserDetail userDetail) {


        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(filter, userDetail);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }

    public List<MetadataInstancesEntity> findAll(Filter filter,String excludeField, UserDetail userDetail) {

        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(filter, excludeField, userDetail);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
     */
    public List<MetadataInstancesEntity> findAll(Sort sort, UserDetail userDetail) {
        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(sort, userDetail);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Object)
     */
    public MetadataInstancesEntity insert(MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        return super.insert(entity, userDetail);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Iterable)
     */
    public List<MetadataInstancesEntity> insert(Iterable<MetadataInstancesEntity> entities, UserDetail userDetail) {
        for (MetadataInstancesEntity entity : entities) {
            gzipFields(entity);
        }
        return super.insert(entities, userDetail);
    }

    /**
     * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
     * type.
     *
     * @param query      required, query document criteria
     * @param userDetail scope
     * @return
     */
    public Optional<MetadataInstancesEntity> findOne(Query query, UserDetail userDetail) {
        Optional<MetadataInstancesEntity> optional = super.findOne(query, userDetail);
        optional.ifPresent(this::unzipFields);
        return optional;
    }

    public Optional<MetadataInstancesEntity> findOne(Query query) {
        Optional<MetadataInstancesEntity> optional = super.findOne(query);
        optional.ifPresent(this::unzipFields);
        return optional;
    }

    /*
     * (non-Javadoc)
     * @see com.tapdata.manager.base.reporitory.BaseRepository#findOne(org.springframework.data.mongodb.core.query.Query, com.tapdata.manager.config.security.UserDetail)
     */
    public Optional<MetadataInstancesEntity> findOne(Where where, UserDetail userDetail) {
        Optional<MetadataInstancesEntity> optional = super.findOne(where, userDetail);
        optional.ifPresent(this::unzipFields);
        return optional;
    }

    public long upsert(Query query, MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        return super.upsert(query, entity, userDetail);
    }

    public long upsert(Query query, MetadataInstancesEntity entity) {

        gzipFields(entity);
        return super.upsert(query, entity);
    }


    public List<MetadataInstancesEntity> findAll(Where where, UserDetail userDetail) {

        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(where, userDetail);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }


    public List<MetadataInstancesEntity> findAll(Where where) {
        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(where);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }

    public UpdateResult updateByWhere(Query query, MetadataInstancesEntity set, MetadataInstancesEntity setOnInsert, Map<String, Object> unset, UserDetail userDetail) {
        if (set != null) {
            gzipFields(set);
        }

        if (setOnInsert != null) {
            gzipFields(setOnInsert);
        }
        return super.updateByWhere(query, set, setOnInsert, unset, userDetail);
    }

    public UpdateResult updateByWhere(Query query, MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        return super.updateByWhere(query, entity, userDetail);
    }

    public List<MetadataInstancesEntity> findAll(Query query) {
        List<MetadataInstancesEntity> metadataInstancesEntities = super.findAll(query);
        if (CollectionUtils.isEmpty(metadataInstancesEntities)) {
            for (MetadataInstancesEntity entity : metadataInstancesEntities) {
                unzipFields(entity);
            }
        }

        return metadataInstancesEntities;
    }

    public UpdateResult update(Query query,  MetadataInstancesEntity entity) {
        gzipFields(entity);
        return super.update(query, entity);
    }

    public MetadataInstancesEntity findAndModify(Query query, Update update, UserDetail userDetail) {
        MetadataInstancesEntity entity = super.findAndModify(query, update, userDetail);
        gzipFields(entity);
        return entity;
    }

    public MetadataInstancesEntity findAndModify(Query query, Update update, FindAndModifyOptions options, UserDetail userDetail) {
        MetadataInstancesEntity entity = super.findAndModify(query, update, options, userDetail);
        gzipFields(entity);
        return entity;
    }

    public MetadataInstancesEntity findAndModify(Query query, Update update, FindAndModifyOptions options) {
        MetadataInstancesEntity entity = super.findAndModify(query, update, options);
        gzipFields(entity);
        return entity;
    }

    public MetadataInstancesEntity replaceById(Query query, MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        entity = super.replaceById(query, entity, userDetail);
        unzipFields(entity);
        return entity;
    }

    public MetadataInstancesEntity replaceOrInsert(Query query, MetadataInstancesEntity entity, UserDetail userDetail) {
        gzipFields(entity);
        entity = super.replaceOrInsert(query, entity, userDetail);
        unzipFields(entity);
        return entity;
    }

    protected void init() {
        super.init();
    }

    public void gzipFields(MetadataInstancesEntity entity) {
        if (entity != null && entity.getFields() != null) {
            if (entity.getFields().size() >= compressionFieldsLength) {
                String json = JsonUtil.toJsonUseJackson(entity.getFields());
                if (StringUtils.isNotBlank(json)) {
                    byte[] gzip = GZIPUtil.gzip(json.getBytes());
                    byte[] encode = Base64.getEncoder().encode(gzip);
                    String dataString = new String(encode, StandardCharsets.UTF_8);
                    entity.setCompressionFields(dataString);
                    entity.setFields(null);
                }
            } else {
                entity.setCompressionFields(null);
            }
        }
    }

    public void unzipFields(MetadataInstancesEntity entity) {
        if (entity != null && StringUtils.isNotBlank(entity.getCompressionFields())) {
            String encryptFields = entity.getCompressionFields();
            byte[] uncompressEncryptData = GZIPUtil.unGzip(Base64.getDecoder().decode(encryptFields));
            String decryptFromUncompressData = new String(uncompressEncryptData, StandardCharsets.UTF_8);

            entity.setFields(JsonUtil.parseJsonUseJackson(decryptFromUncompressData, new TypeReference<List<com.tapdata.tm.commons.schema.Field>>() {}));
            entity.setCompressionFields(null);
        }
    }
}
