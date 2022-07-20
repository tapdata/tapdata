package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class MetaDataHistoryService {

    @Autowired
    private MongoTemplate mongoTemplate;
    public void saveHistory(MetadataInstancesDto metadataInstancesDto) {
        metadataInstancesDto.setId(null);
        metadataInstancesDto.setHistories(null);
        metadataInstancesDto.setVersionTime(new Date());
        mongoTemplate.insert(metadataInstancesDto, "MetaDataHistory");
    }

    public void saveHistory(List<MetadataInstancesDto> metadataInstancesDtos) {
        if (CollectionUtils.isEmpty(metadataInstancesDtos)) {
            return;
        }

        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, MetadataInstancesDto.class, "MetaDataHistory");
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            metadataInstancesDto.setId(null);
            metadataInstancesDto.setHistories(null);
            metadataInstancesDto.setVersionTime(new Date());
            bulkOperations.insert(metadataInstancesDto);
        }

        bulkOperations.execute();
    }

    public static void main(String[] args) {
        Document updateObject = new Document();
        Document update = new Document();
        Document set = new Document();
        Document push = new Document();
        set.put("version", "");
        ArrayList<MetadataInstancesDto> hisModels = new ArrayList<>();
        MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        metadataInstancesDto.setDatabaseId("kaskdlfasd");
        hisModels.add(metadataInstancesDto);
        Document basicDBObject = new Document("$each", hisModels);
        basicDBObject.append("$slice", -5);
        push.put("histories", basicDBObject);
        set.put("fields", new ArrayList<>());
        set.put("indices", new ArrayList<>());
        set.put("is_deleted", false);
        set.put("createSource", "AUTO");
        updateObject.put("$set", set);
        updateObject.put("$push", push);
        update.put("updateObject", updateObject);
        Update update1 = Update.fromDocument(update);
        System.out.println(update1.getUpdateObject());
    }

    /**
     *
     * @param qualifiedName 唯一名称
     * @param time 最近时间戳
     * @param order true 所传时间戳之前第一条数据   false 所传时间之后第一条数据
     * @return
     */
    public MetadataInstancesDto findByVersionTime(String qualifiedName, long time, Boolean order) {
        Criteria criteria = Criteria.where("qualifiedName").is(qualifiedName);
        Sort.Direction direction;
        if (order) {
            criteria.and("versionTime").gte(time);
            direction = Sort.Direction.DESC;
        } else {
            criteria.and("versionTime").lte(time);
            direction = Sort.Direction.ASC;
        }

        Query query = new Query(criteria);

        query.with(Sort.by(direction, "versionTime"));
        return mongoTemplate.findOne(query, MetadataInstancesDto.class, "MetaDataHistory");
    }

}
