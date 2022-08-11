package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.vo.TaskRecordListVo;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskRecordServiceImpl implements TaskRecordService {
    private MongoTemplate mongoTemplate;
    private BaseService<TaskDto, TaskEntity, ObjectId, TaskRepository> baseService;

    @Override
    public void createRecord(TaskRecord taskRecord) {
        mongoTemplate.save(taskRecord);
    }

    @Override
    public void updateTaskStatus(SyncTaskStatusDto dto) {
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(dto.getTaskRecordId())));
        String taskStatus = dto.getTaskStatus();
        Update update = new Update().set("taskSnapshot.status", taskStatus)
                .set("taskSnapshot.last_updated", new Date());
        if (StringUtils.equals(TaskDto.STATUS_RUNNING, taskStatus)) {
            update.set("taskSnapshot.startTime", new Date());
        }

        mongoTemplate.updateFirst(query, update, TaskRecord.class);
    }

    @Override
    public Page<TaskRecordListVo> queryRecords(String taskId, String offset, Integer limit) {
        Query query = new Query(Criteria.where("taskId").is(taskId));
        long count = mongoTemplate.count(query, TaskRecord.class);
        if (count == 0) {
            return new Page<TaskRecordListVo>(){{setTotal(count);}};
        }

        if (StringUtils.isNotBlank(offset)) {
            query.addCriteria(Criteria.where("_id").gt(offset));
        }
        query.limit(limit);
        List<TaskRecord> taskRecords = mongoTemplate.find(query, TaskRecord.class);

        List<TaskRecordListVo> collect = taskRecords.stream().map(r -> {
            TaskRecordListVo vo = new TaskRecordListVo();
            vo.setTaskId(r.getTaskId());
            vo.setTaskRecordId(r.getId().toHexString());
            TaskEntity taskSnapshot = r.getTaskSnapshot();
            vo.setStartDate(DateUtil.toLocalDateTime(taskSnapshot.getStartTime()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
            vo.setEndDate(DateUtil.toLocalDateTime(taskSnapshot.getLastUpdAt()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
            vo.setStatus(taskSnapshot.getStatus());

            vo.setOffset(vo.getTaskRecordId());
            return vo;
        }).collect(Collectors.toList());

        Page<TaskRecordListVo> result = new Page<>();
        result.setItems(collect);
        result.setTotal(count);

        return result;
    }

    @Override
    public TaskDto queryTask(String taskRecordId, String userId) {
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(taskRecordId)).and("user_id").is(userId));
        TaskRecord taskRecord = mongoTemplate.findOne(query, TaskRecord.class);
        if (taskRecord == null) {
            return null;
        }
        TaskDto taskDto = new TaskDto();
        BeanUtil.copyProperties(taskRecord.getTaskSnapshot(), taskDto);

        return taskDto;
    }
}
