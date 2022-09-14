package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.vo.TaskRecordListVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskRecordServiceImpl implements TaskRecordService {
    private MongoTemplate mongoTemplate;
    private MeasurementServiceV2 measurementServiceV2;
    private UserService userService;

    @Override
    public void createRecord(TaskRecord taskRecord) {
        mongoTemplate.save(taskRecord);
    }

    @Override
    public void updateTaskStatus(SyncTaskStatusDto dto) {
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(dto.getTaskRecordId())));
        String taskStatus = dto.getTaskStatus();
        Update update = new Update().set("taskSnapshot.status", taskStatus);

        Date now = new Date();
        if (StringUtils.equals(TaskDto.STATUS_RUNNING, taskStatus)) {
            update.set("taskSnapshot.startTime", now);
        } else {
            update.set("taskSnapshot.last_updated", now);
        }

        mongoTemplate.updateFirst(query, update, TaskRecord.class);
    }

    @Override
    public Page<TaskRecordListVo> queryRecords(String taskId, Integer page, Integer size) {
        Query query = new Query(Criteria.where("taskId").is(taskId));
        long count = mongoTemplate.count(query, TaskRecord.class);
        if (count == 0) {
            return new Page<>(0, Collections.emptyList());
        }

        query.with(Sort.by("createTime").descending());
        List<TaskRecord> taskRecordList = mongoTemplate.find(query, TaskRecord.class);

        if (size > 20) {
            size = 20;
        }

        List<List<TaskRecord>> partition = ListUtils.partition(taskRecordList, size);
        if (page > partition.size()) {
            page = partition.size();
        }

        List<TaskRecord> taskRecords = partition.get(page - 1);

        List<String> ids = taskRecords.stream().map(t -> t.getId().toHexString()).collect(Collectors.toList());
        Map<String, Long[]> totalMap = measurementServiceV2.countEventByTaskRecord(taskId, ids);

        List<String> userIds = taskRecords.stream().map(TaskRecord::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> users = userService.getUserByIdList(userIds);
        Map<String, String> userMap = users.stream().collect(Collectors.toMap(UserDetail::getUserId, u -> {
            if (StringUtils.isNotBlank(u.getUsername())) {
                return u.getUsername();
            } else {
                return u.getEmail();
            }
        }));


        List<TaskRecordListVo> collect = taskRecords.stream().map(r -> {
            String taskRecordId = r.getId().toHexString();

            TaskRecordListVo vo = new TaskRecordListVo();
            vo.setTaskId(r.getTaskId());
            vo.setTaskRecordId(taskRecordId);
            TaskEntity taskSnapshot = r.getTaskSnapshot();
            if (Objects.nonNull(taskSnapshot.getStartTime())) {
                vo.setStartDate(DateUtil.toLocalDateTime(taskSnapshot.getStartTime()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
            }
            List<String> of = Lists.of(TaskDto.STATUS_ERROR, TaskDto.STATUS_COMPLETE, TaskDto.STATUS_STOP);
            if (Objects.nonNull(taskSnapshot.getLastUpdAt()) && of.contains(taskSnapshot.getStatus())) {
                vo.setEndDate(DateUtil.toLocalDateTime(taskSnapshot.getLastUpdAt()).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
            }
            vo.setStatus(taskSnapshot.getStatus());

            if (totalMap.containsKey(taskRecordId)) {
                vo.setInputTotal(totalMap.get(taskRecordId)[0]);
                vo.setOutputTotal(totalMap.get(taskRecordId)[1]);
            }

            if (userMap.containsKey(r.getUserId())) {
                vo.setOperator(userMap.get(r.getUserId()));
            }

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
