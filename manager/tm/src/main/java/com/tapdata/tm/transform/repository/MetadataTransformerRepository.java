package com.tapdata.tm.transform.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.transform.entity.MetadataTransformerEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Streamable;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2022/03/04
 * @Description:
 */
@Repository
public class MetadataTransformerRepository extends BaseRepository<MetadataTransformerEntity, ObjectId> {
    public MetadataTransformerRepository(MongoTemplate mongoOperations) {
        super(MetadataTransformerEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public List<MetadataTransformerEntity> saveAll(Iterable<MetadataTransformerEntity> entities) {
        Assert.notNull(entities, "The given Iterable of entities not be null!");

        Streamable<MetadataTransformerEntity> source = Streamable.of(entities);

        // source.forEach(entity -> applyUserDetail(entity, userDetail));

        boolean allNew = source.stream().allMatch(entityInformation::isNew);

        if (allNew) {
            List<MetadataTransformerEntity> result = source.stream().peek(entity -> {
            }).collect(Collectors.toList());
            return new ArrayList<>(mongoOperations.insert(result, entityInformation.getCollectionName()));
        }

        return source.stream().map(this::save).collect(Collectors.toList());
    }

    public MetadataTransformerEntity save(MetadataTransformerEntity entity) {
        Assert.notNull(entity, "Entity must not be null!");

        if (entityInformation.isNew(entity)) {
            return mongoOperations.insert(entity, entityInformation.getCollectionName());
        }

        //return mongoOperations.save(entity, entityInformation.getCollectionName());
        // mongoOperations.updateFirst()
        Query query = getIdQuery(entity.getId());

        Update update = buildUpdateSet(entity);

        UpdateResult result = mongoOperations.updateFirst(query, update, entityInformation.getJavaType());

        if (result.getMatchedCount() == 1) {
            return findOne(query).orElse(entity);
        }
        return entity;
    }
}
