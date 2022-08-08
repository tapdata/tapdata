package com.tapdata.tm.monitoringlogs.service;

import com.mongodb.bulk.BulkWriteResult;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.monitoringlogs.entity.MonitoringLogsEntity;
import com.tapdata.tm.monitoringlogs.repository.MonitoringLogsRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.transform.entity.MetadataTransformerItemEntity;
import com.tapdata.tm.utils.Lists;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author:
 * @Date: 2022/06/20
 * @Description:
 */
@Service
@Slf4j
public class MonitoringLogsService extends BaseService<MonitoringLogsDto, MonitoringLogsEntity, ObjectId, MonitoringLogsRepository> {
    public MonitoringLogsService(@NonNull MonitoringLogsRepository repository) {
        super(repository, MonitoringLogsDto.class, MonitoringLogsEntity.class);
    }

    protected void beforeSave(MonitoringLogsDto monitoringLogs, UserDetail user) {

    }

    public List<MonitoringLogsDto> batchSave(List<MonitoringLogsDto> monitoringLoges, UserDetail user) {

        BulkOperations bulkOperations = repository.getMongoOperations().bulkOps(BulkOperations.BulkMode.UNORDERED, MonitoringLogsEntity.class);
        for (MonitoringLogsDto monitoringLoge : monitoringLoges) {
            beforeSave(monitoringLoge, user);
        }

        List<MonitoringLogsEntity> monitoringLogsEntities = convertToEntity(MonitoringLogsEntity.class, monitoringLoges);

        for (MonitoringLogsEntity monitoringLogsEntity : monitoringLogsEntities) {
            repository.applyUserDetail(monitoringLogsEntity, user);
            bulkOperations.insert(monitoringLogsEntity);
        }

        BulkWriteResult execute = bulkOperations.execute();
        return Lists.newArrayList();

    }
}