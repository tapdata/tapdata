package com.tapdata.tm.ds.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.utils.AES256Util;
import com.tapdata.tm.utils.GZIPUtil;
import org.apache.commons.net.util.Base64;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @Author: Zed
 * @Date: 2021/8/24
 * @Description:
 */
@Repository
public class DataSourceRepository extends BaseRepository<DataSourceEntity, ObjectId> {
    public DataSourceRepository(MongoTemplate mongoOperations) {
        super(DataSourceEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public DataSourceEntity importEntity(DataSourceEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }

    @Override
    public DataSourceEntity save(DataSourceEntity entity, UserDetail userDetail) {
        encryptConfig(entity);
        return super.save(entity, userDetail);
    }

    @Override
    public List<DataSourceEntity> saveAll(Iterable<DataSourceEntity> dataSourceEntities, UserDetail userDetail) {
        dataSourceEntities.forEach(this::encryptConfig);
        return super.saveAll(dataSourceEntities, userDetail);
    }

    @Override
    public Optional<DataSourceEntity> findOne(Query query, UserDetail userDetail) {
        Optional<DataSourceEntity> optional = super.findOne(query, userDetail);
        optional.ifPresent(this::decryptConfig);
        return optional;
    }

    @Override
    public Optional<DataSourceEntity> findOne(Query query) {
        Optional<DataSourceEntity> optional = super.findOne(query);
        optional.ifPresent(this::decryptConfig);
        return optional;
    }

    @Override
    public List<DataSourceEntity> findAll(Query query) {
        List<DataSourceEntity> list = super.findAll(query);
        if (list != null) {
            list.forEach(this::decryptConfig);
        }
        return list;
    }

    @Override
    public List<DataSourceEntity> findAll(Query query, UserDetail userDetail) {
        List<DataSourceEntity> list = super.findAll(query, userDetail);

        if (list != null) {
            list.forEach(this::decryptConfig);
        }
        return list;
    }

    @Override
    public List<DataSourceEntity> findAll(Where where, UserDetail userDetail) {
        List<DataSourceEntity> list = super.findAll(where, userDetail);
        if (list != null) {
            list.forEach(this::decryptConfig);
        }
        return list;
    }

    @Override
    public List<DataSourceEntity> findAll(Where where) {
        List<DataSourceEntity> list = super.findAll(where);
        if (list != null) {
            list.forEach(this::decryptConfig);
        }
        return list;
    }

    @Override
    public DataSourceEntity insert(DataSourceEntity entity, UserDetail userDetail) {
        this.encryptConfig(entity);
        return super.insert(entity, userDetail);
    }

    @Override
    public List<DataSourceEntity> insert(Iterable<DataSourceEntity> dataSourceEntities, UserDetail userDetail) {
        dataSourceEntities.forEach(this::encryptConfig);
        return super.insert(dataSourceEntities, userDetail);
    }

    @Override
    public long upsert(Query query, DataSourceEntity entity) {
        this.encryptConfig(entity);
        return super.upsert(query, entity);
    }

    @Override
    public long upsert(Query query, DataSourceEntity entity, UserDetail userDetail) {
        this.encryptConfig(entity);
        return super.upsert(query, entity, userDetail);
    }

    @Override
    public UpdateResult updateByWhere(Query query, DataSourceEntity entity, UserDetail userDetail) {
        this.encryptConfig(entity);
        return super.updateByWhere(query, entity, userDetail);
    }

    @Override
    public UpdateResult updateByWhere(Query query, DataSourceEntity set, DataSourceEntity setOnInsert, Map<String, Object> unset, UserDetail userDetail) {
        this.encryptConfig(set);
        this.encryptConfig(setOnInsert);
        return super.updateByWhere(query, set, setOnInsert, unset, userDetail);
    }

    public void encryptConfig(DataSourceEntity entity) {
        //AES256Util.Aes256Encode()
        if (entity != null && entity.getConfig() != null) {
            String configData = JsonUtil.toJsonUseJackson(entity.getConfig());
            String encryptData = AES256Util.Aes256Encode(configData);

            byte[] compressEncryptData = GZIPUtil.gzip(encryptData.getBytes());
            String encodeCompressEncryptData = Base64.encodeBase64String(compressEncryptData);

            entity.setEncryptConfig(encodeCompressEncryptData);
            entity.setConfig(null);
        }
    }

    public void decryptConfig(DataSourceEntity entity) {
        //AES256Util.Aes256Encode()
        if (entity != null && entity.getEncryptConfig() != null) {
            String encodeCompressEncryptData = entity.getEncryptConfig();
            byte[] uncompressEncryptData = GZIPUtil.unGzip(Base64.decodeBase64(encodeCompressEncryptData));
            String decryptFromUncompressData = AES256Util.Aes256Decode(new String(uncompressEncryptData));

            entity.setConfig(JsonUtil.parseJsonUseJackson(decryptFromUncompressData, Map.class));
            entity.setEncryptConfig(null);
        }
    }

}
