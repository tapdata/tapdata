package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.SubTaskService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.entity.schema.TapTable;
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
    @Autowired
    private MetadataInstancesService metadataInstancesService;
    public void saveHistory(MetadataInstancesDto metadataInstancesDto) {
        metadataInstancesDto.setId(null);
        metadataInstancesDto.setHistories(null);
        metadataInstancesDto.setVersionTime(new Date());
        mongoTemplate.insert(metadataInstancesDto, "MetaDataHistory");
    }

    public void saveHistory(List<MetadataInstancesDto> metadataInstancesDtos, String taskId) {
        if (CollectionUtils.isEmpty(metadataInstancesDtos)) {
            return;
        }

        SubTaskService subTaskService = SpringContextHelper.getBean(SubTaskService.class);
        SubTaskDto subTaskDto = subTaskService.checkExistById(MongoUtils.toObjectId(taskId), "tmCurrentTime");



        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, MetadataInstancesDto.class, "MetaDataHistory");
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            metadataInstancesDto.setId(null);
            metadataInstancesDto.setHistories(null);
            metadataInstancesDto.setTaskId(taskId);
            metadataInstancesDto.setTmCurrentTime(subTaskDto.getTmCurrentTime());
            bulkOperations.insert(metadataInstancesDto);
        }

        bulkOperations.execute();
    }

    /**
     *
     * @param qualifiedName 唯一名称
     * @param time 最近时间戳
     * @return
     */
    public TapTable findByVersionTime(String qualifiedName, Long time, UserDetail user) {
        Criteria criteria = Criteria.where("qualifiedName").is(qualifiedName);
        criteria.and("tmCurrentTime").is(time);

        Query query = new Query(criteria);
        MetadataInstancesDto metaDataHistory = mongoTemplate.findOne(query, MetadataInstancesDto.class, "MetaDataHistory");
        if (metaDataHistory == null) {
            metaDataHistory = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user);
        }

        return PdkSchemaConvert.toPdk(metaDataHistory);
    }


    /**
     *
     * @param time 最近时间戳
     * @return
     */
    public void clean(String taskId, Long time) {
        Criteria criteria = Criteria.where("taskId").is(taskId);
        criteria.and("tmCurrentTime").gt(time);

        Query query = new Query(criteria);
        mongoTemplate.remove(query, "MetaDataHistory");
    }

}
