package com.tapdata.tm.ds.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.utils.DSConfigUtil;
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

        encryptConfig(entity);
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

    @Override
    public Update buildUpdateSet(DataSourceEntity entity, UserDetail userDetail) {
        this.encryptConfig(entity);
        return super.buildUpdateSet(entity, userDetail);
    }

    public void encryptConfig(DataSourceEntity entity) {
        //AES256Util.Aes256Encode()
        if (entity != null && entity.getConfig() != null) {
            String configData;
            try {
                configData = JsonUtil.toJsonUseJackson(entity.getConfig());
            } catch (Exception e) {
                throw new RuntimeException("Parse config map to json string failed: " + entity.getConfig(), e);
            }
            String encodeCompressEncryptData;
            try {
                encodeCompressEncryptData = DSConfigUtil.encrypt(configData);
            } catch (Exception e) {
                throw new RuntimeException("Encrypt config failed: " + configData, e);
            }
            entity.setEncryptConfig(encodeCompressEncryptData);
            entity.setConfig(null);
        }
    }

    public void decryptConfig(DataSourceEntity entity) {
        //AES256Util.Aes256Encode()
        if (entity != null && entity.getEncryptConfig() != null) {
            String encodeCompressEncryptData = entity.getEncryptConfig();
            String decrypt;
            try {
                decrypt = DSConfigUtil.decrypt(encodeCompressEncryptData);
            } catch (Exception e) {
                throw new RuntimeException("Decrypt config failed, encrypt config: " + encodeCompressEncryptData, e);
            }
            try {
                entity.setConfig(JsonUtil.parseJsonUseJackson(decrypt, Map.class));
            } catch (Exception e) {
                throw new RuntimeException("Parse decrypt config to map failed: " + decrypt, e);
            }
            entity.setEncryptConfig(null);
        }
    }

}
