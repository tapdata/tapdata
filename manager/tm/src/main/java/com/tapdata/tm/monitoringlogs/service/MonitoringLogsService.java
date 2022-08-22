package com.tapdata.tm.monitoringlogs.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.manager.common.utils.IOUtils;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.monitoringlogs.entity.MonitoringLogsEntity;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogExportParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.repository.MonitoringLogsRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipOutputStream;

/**
 * @Author:
 * @Date: 2022/06/20
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MonitoringLogsService extends BaseService<MonitoringLogsDto, MonitoringLogsEntity, ObjectId, MonitoringLogsRepository> {
    private MongoTemplate mongoOperations;

    public MonitoringLogsService(@NonNull MonitoringLogsRepository repository) {
        super(repository, MonitoringLogsDto.class, MonitoringLogsEntity.class);
    }

    protected void beforeSave(MonitoringLogsDto monitoringLogs, UserDetail user) {

    }

    public void batchSave(List<MonitoringLogsDto> monitoringLoges, UserDetail user) {

        BulkOperations bulkOperations = repository.getMongoOperations().bulkOps(BulkOperations.BulkMode.UNORDERED, MonitoringLogsEntity.class);
        for (MonitoringLogsDto monitoringLoge : monitoringLoges) {
            beforeSave(monitoringLoge, user);
        }

        List<MonitoringLogsEntity> monitoringLogsEntities = convertToEntity(MonitoringLogsEntity.class, monitoringLoges);

        for (MonitoringLogsEntity monitoringLogsEntity : monitoringLogsEntities) {
            repository.applyUserDetail(monitoringLogsEntity, user);
            bulkOperations.insert(monitoringLogsEntity);
        }

        bulkOperations.execute();
    }

    public Page<MonitoringLogsDto> query(MonitoringLogQueryParam param) {
        if (null == param.getTaskId()) {
            return null;
        }

        Criteria criteria = Criteria.where("taskId").is(param.getTaskId());
        if (StringUtils.isNotBlank(param.getTaskRecordId())) {
            criteria.and("taskRecordId").is(param.getTaskRecordId());
        }

        criteria.and("date").gte(new Date(param.getStart())).lt(new Date(param.getEnd()));

        if (StringUtils.isNotEmpty(param.getNodeId())) {
            criteria.and("nodeId").is(param.getNodeId());
        }

        // keyword search filter
        if (StringUtils.isNotEmpty(param.getSearch())) {
            String search = param.getSearch();
            criteria.orOperator(
                    new Criteria("taskName").regex(search),
                    new Criteria("nodeName").regex(search),
                    new Criteria("message").regex(search),
                    new Criteria("errorStack").regex(search),
                    new Criteria("logTags").elemMatch(new Criteria("$regex").is(search))
            );
        }


        // log level filter
        List<String> levels = param.getLevels();
        if (null == levels) {
            levels = param.getFullLevels();
        }
        criteria.and("level").in(levels);

        // sort and _id filter, avoid duplicate data
        Sort sort = Sort.by("date");
        switch (param.getOrder()) {
            case MonitoringLogQueryParam.ORDER_ASC:
                sort = sort.ascending();
                break;
            case MonitoringLogQueryParam.ORDER_DESC:
                sort = sort.descending();
                break;
        }

        Query query = new Query(criteria);
        query.with(sort);

        long total = mongoOperations.count(query, MonitoringLogsEntity.class);
        if (total == 0) {
            return new Page<>(0, new ArrayList<>());
        }

        // query limitation
        query.skip((param.getPage() - 1) * param.getPageSize());
        query.limit(param.getPageSize().intValue());
        List<MonitoringLogsEntity> logEntities = mongoOperations.find(query, MonitoringLogsEntity.class);
        List<MonitoringLogsDto> logs = convertToDto(logEntities, MonitoringLogsDto.class);

        return new Page<>(total, logs);
    }


    public void export (MonitoringLogExportParam param, ZipOutputStream stream) {
        if (null == param.getTaskId()) {
            return;
        }

        Criteria criteria = Criteria.where("taskId").is(param.getTaskId());
        if (StringUtils.isNotBlank(param.getTaskRecordId())) {
            criteria.and("taskRecordId").is(param.getTaskRecordId());
        }

        criteria.and("date").gte(new Date(param.getStart())).lt(new Date(param.getEnd()));

        Query query = new Query(criteria);
        query.with(Sort.by("date").ascending());

        CloseableIterator<MonitoringLogsEntity> iter = mongoOperations.stream(query, MonitoringLogsEntity.class);
        AtomicLong count = new AtomicLong();
        iter.forEachRemaining(logEntity -> {
            try {
                MonitoringLogsDto logDto = convertToDto(logEntity, MonitoringLogsDto.class);
                count.addAndGet(1);

                stream.write((logDto.formatMonitoringLog() + "\n").getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        });
        IOUtils.closeQuietly(iter);
        if (count.get() == 0) {
            try {
                stream.write(("Can't find any logs by query " + param).getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        }
    }

    //

    public void startTaskMonitoringLog(TaskDto taskDto, UserDetail user) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        long now = System.currentTimeMillis();
        builder.taskId(taskDto.getId().toHexString())
                .taskName(taskDto.getName())
                .taskRecordId(taskDto.getTaskRecordId())
                .date(new Date(now))
                .timestamp(now)
                .level("INFO")
                .message("Start task...")
                ;


        save(builder.build(), user);
    }

    public void agentAssignMonitoringLog(TaskDto taskDto, String assigned, Integer available, UserDetail user) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        long now = System.currentTimeMillis();
        builder.taskId(taskDto.getId().toHexString())
                .taskName(taskDto.getName())
                .taskRecordId(taskDto.getTaskRecordId())
                .date(new Date(now))
                .timestamp(now)
                .logTag("Agent Available Check")
                .level("INFO")
        ;
        if (available != null) {
            builder.message(String.format("%s agents are available now, task is assigned to agent %s", available, assigned));
        } else {
            builder.message(String.format("Task is assigned to agent %s manually", assigned));
        }

        save(builder.build(), user);
    }

}