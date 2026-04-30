package com.tapdata.tm.ds.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.utils.DSConfigUtil;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger log = LoggerFactory.getLogger(DataSourceRepository.class);

    public DataSourceRepository(MongoTemplate mongoOperations) {
        super(DataSourceEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public DataSourceEntity importEntity(DataSourceEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        // 保留导出文件中原始的 userId 和 createUser，避免被当前操作用户覆盖
        String originalUserId = entity.getUserId();
        String originalCreateUser = entity.getCreateUser();

        encryptConfig(entity);
        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);

        // 恢复导出环境的 userId 和 createUser（与导出数据保持一致）
        if (originalUserId != null && !originalUserId.isEmpty()) {
            entity.setUserId(originalUserId);
        }
        if (originalCreateUser != null && !originalCreateUser.isEmpty()) {
            entity.setCreateUser(originalCreateUser);
        }

        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }

    /**
     * 导入场景的 save：对已有文档执行 update 但保留原始 userId 和 createUser。
     * 对新文档委托给 importEntity()。
     */
    public DataSourceEntity importSave(DataSourceEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        log.info("[importSave] called for entity id={}, isNew={}, entity.userId={}, entity.createUser={}, operator.userId={}",
                entity.getId(), entityInformation.isNew(entity), entity.getUserId(), entity.getCreateUser(),
                userDetail != null ? userDetail.getUserId() : "null");

        if (entityInformation.isNew(entity)) {
            log.info("[importSave] entity is new, delegating to importEntity");
            return importEntity(entity, userDetail);
        }

        // 保留原始值
        String originalUserId = entity.getUserId();
        String originalCreateUser = entity.getCreateUser();
        log.info("[importSave] preserved originalUserId={}, originalCreateUser={}", originalUserId, originalCreateUser);

        // 与 DataSourceRepository.save() 一致：先加密
        encryptConfig(entity);

        // 与 BaseRepository.save() 更新路径一致
        beforeUpdateEntity(entity, userDetail);
        Query query = getIdQuery(entity.getId());
        applyUserDetail(query, userDetail);
        Update update = buildUpdateSet(entity, userDetail);

        log.info("[importSave] after buildUpdateSet: entity.userId={}, update=$set keys={}",
                entity.getUserId(), update.getUpdateObject().get("$set"));

        // 覆盖回原始 userId 和 createUser（buildUpdateSet 中 applyUserDetail 已将其改为 admin）
        if (originalUserId != null && !originalUserId.isEmpty()) {
            update.set("userId", originalUserId);
            entity.setUserId(originalUserId);
        }
        if (originalCreateUser != null && !originalCreateUser.isEmpty()) {
            update.set("createUser", originalCreateUser);
            entity.setCreateUser(originalCreateUser);
        }

        log.info("[importSave] final update object: {}", update.getUpdateObject());

        UpdateResult result = mongoOperations.updateFirst(query, update, entityInformation.getJavaType());

        log.info("[importSave] updateFirst result: matchedCount={}, modifiedCount={}, query={}",
                result.getMatchedCount(), result.getModifiedCount(), query);

        if (result.getMatchedCount() == 1) {
            return findOne(query, userDetail).orElse(entity);
        }
        log.warn("[importSave] updateFirst matched 0 documents! Returning entity as-is. entity.userId={}", entity.getUserId());
        return entity;
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
