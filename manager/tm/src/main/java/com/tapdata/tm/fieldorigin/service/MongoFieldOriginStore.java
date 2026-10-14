package com.tapdata.tm.fieldorigin.service;

import com.tapdata.tm.fieldorigin.entity.FieldOriginEntity;
import com.tapdata.tm.fieldorigin.entity.FieldOriginVersionEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

@Slf4j
@Component
public class MongoFieldOriginStore implements FieldOriginStore {
    private static final String TASK_ID = "taskId";
    private static final String VERSION = "version";
    private static final String STATUS = "status";
    private static final String ACTIVE = "active";
    private static final String NORMALIZED_FIELD_PATH = "normalizedFieldPath";
    private static final int INSERT_BATCH = 1000;
    private final MongoTemplate mongoTemplate;
    private final AtomicBoolean indexed = new AtomicBoolean(false);

    public MongoFieldOriginStore(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    void ensureIndexes() {
        if (!indexed.compareAndSet(false, true)) {
            return;
        }
        try {
            mongoTemplate.indexOps(FieldOriginVersionEntity.class)
                    .ensureIndex(new Index().on(TASK_ID, Sort.Direction.ASC).on(VERSION, Sort.Direction.DESC).unique());
            mongoTemplate.indexOps(FieldOriginEntity.class)
                    .ensureIndex(new Index().on(TASK_ID, Sort.Direction.ASC).on(VERSION, Sort.Direction.ASC)
                            .on(NORMALIZED_FIELD_PATH, Sort.Direction.ASC));
        } catch (Exception e) {
            indexed.set(false);
            log.warn("Create field origin indexes failed: {}", e.getMessage());
        }
    }

    @Override
    public long nextVersion(String taskId) {
        ensureIndexes();
        Query query = Query.query(Criteria.where(TASK_ID).is(taskId)).with(Sort.by(Sort.Direction.DESC, VERSION)).limit(1);
        FieldOriginVersionEntity latest = mongoTemplate.findOne(query, FieldOriginVersionEntity.class);
        return null == latest || null == latest.getVersion() ? 1L : latest.getVersion() + 1;
    }

    @Override
    public FieldOriginVersionEntity findActive(String taskId) {
        return mongoTemplate.findOne(Query.query(Criteria.where(TASK_ID).is(taskId).and(ACTIVE).is(true)), FieldOriginVersionEntity.class);
    }

    @Override
    public FieldOriginVersionEntity findVersion(String taskId, long version) {
        return mongoTemplate.findOne(Query.query(Criteria.where(TASK_ID).is(taskId).and(VERSION).is(version)), FieldOriginVersionEntity.class);
    }

    @Override
    public FieldOriginVersionEntity findLatestBuild(String taskId) {
        Query query = Query.query(Criteria.where(TASK_ID).is(taskId)).with(Sort.by(Sort.Direction.DESC, VERSION)).limit(1);
        return mongoTemplate.findOne(query, FieldOriginVersionEntity.class);
    }

    @Override
    public void saveVersion(FieldOriginVersionEntity version) {
        mongoTemplate.save(version);
    }

    @Override
    public void insertRecords(List<FieldOriginEntity> records) {
        for (int i = 0; i < records.size(); i += INSERT_BATCH) {
            mongoTemplate.insert(records.subList(i, Math.min(records.size(), i + INSERT_BATCH)), FieldOriginEntity.class);
        }
    }

    @Override
    public void deleteRecords(String taskId, long version) {
        mongoTemplate.remove(Query.query(Criteria.where(TASK_ID).is(taskId).and(VERSION).is(version)), FieldOriginEntity.class);
    }

    @Override
    public void activate(String taskId, long version) {
        mongoTemplate.updateFirst(Query.query(Criteria.where(TASK_ID).is(taskId).and(VERSION).is(version)),
                Update.update(ACTIVE, true), FieldOriginVersionEntity.class);
        mongoTemplate.updateMulti(Query.query(Criteria.where(TASK_ID).is(taskId).and(VERSION).ne(version).and(ACTIVE).is(true)),
                Update.update(ACTIVE, false), FieldOriginVersionEntity.class);
    }

    @Override
    public List<FieldOriginEntity> findRecords(String taskId, long version, String targetConnectionId, String targetTable, Collection<String> normalizedPaths) {
        Criteria criteria = Criteria.where(TASK_ID).is(taskId).and(VERSION).is(version);
        if (StringUtils.isNotBlank(targetConnectionId)) {
            criteria = criteria.and("targetConnectionId").is(targetConnectionId);
        }
        if (StringUtils.isNotBlank(targetTable)) {
            criteria = criteria.and("targetTable").is(targetTable);
        }
        if (null != normalizedPaths) {
            if (CollectionUtils.isEmpty(normalizedPaths)) {
                return new ArrayList<>();
            }
            List<Criteria> paths = new ArrayList<>();
            paths.add(Criteria.where(NORMALIZED_FIELD_PATH).in(normalizedPaths));
            for (String path : normalizedPaths) {
                paths.add(Criteria.where(NORMALIZED_FIELD_PATH).regex("^" + Pattern.quote(path) + "(\\.|\\[\\]\\.)"));
            }
            criteria = criteria.orOperator(paths.toArray(new Criteria[0]));
        }
        return mongoTemplate.find(Query.query(criteria), FieldOriginEntity.class);
    }

    @Override
    public List<FieldOriginVersionEntity> findPrunable(String taskId, int keep) {
        Query query = Query.query(Criteria.where(TASK_ID).is(taskId)).with(Sort.by(Sort.Direction.DESC, VERSION));
        List<FieldOriginVersionEntity> versions = mongoTemplate.find(query, FieldOriginVersionEntity.class);
        return FieldOriginVersions.prunable(versions, keep);
    }

    @Override
    public void deleteVersion(String taskId, long version) {
        deleteRecords(taskId, version);
        mongoTemplate.remove(Query.query(Criteria.where(TASK_ID).is(taskId).and(VERSION).is(version)), FieldOriginVersionEntity.class);
    }
}
