package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.bean.TaskRecordDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.vo.TaskRecordListVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
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
        update.set("taskSnapshot.last_updated", now);
        update.push("statusStack", new TaskRecord.TaskStatusUpdate(taskStatus, now));
        mongoTemplate.updateFirst(query, update, TaskRecord.class);
    }

    @Override
    public Page<TaskRecordListVo> queryRecords(TaskRecordDto dto) {
        String taskId = dto.getTaskId();
        Integer page = dto.getPage();
        Integer size = dto.getSize();

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
        List<String> userIds = taskRecords.stream().map(TaskRecord::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> users = userService.getUserByIdList(userIds);
        Map<String, String> userMap = users.stream().collect(Collectors.toMap(UserDetail::getUserId, UserDetail::getUsername, (o1,o2) -> o1));

        List<TaskRecordListVo> collect = taskRecords.stream().map(r -> {
            String taskRecordId = r.getId().toHexString();

            TaskRecordListVo vo = new TaskRecordListVo();
            vo.setTaskId(r.getTaskId());
            vo.setTaskRecordId(taskRecordId);
            TaskEntity taskSnapshot = r.getTaskSnapshot();
            vo.setStatus(taskSnapshot.getStatus());

            if (userMap.containsKey(r.getUserId())) {
                vo.setOperator(userMap.get(r.getUserId()));
            }

            List<TaskRecord.TaskStatusUpdate> statusStack = r.getStatusStack();
            AtomicReference<Date> endDate = new AtomicReference<>();
            if (CollectionUtils.isNotEmpty(statusStack)) {
                Map<String, List<Date>> statusDateMap = statusStack.stream()
                        .collect(Collectors.groupingBy(TaskRecord.TaskStatusUpdate::getStatus,
                                Collectors.mapping(TaskRecord.TaskStatusUpdate::getTimestamp, Collectors.toList())));
                List<Date> startDates = statusDateMap.get(TaskDto.STATUS_RUNNING);
                if (CollectionUtils.isNotEmpty(startDates)) {
                    startDates.stream().max(Date::compareTo).ifPresent(date -> {
                        vo.setLastStartDate(DateUtil.toLocalDateTime(date).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
                    });

                    startDates.stream().min(Date::compareTo).ifPresent(date -> {
                        vo.setStartDate(DateUtil.toLocalDateTime(date).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
                    });
                }

                List<String> of = Lists.of(TaskDto.STATUS_ERROR, TaskDto.STATUS_COMPLETE, TaskDto.STATUS_STOP);
                if (of.contains(taskSnapshot.getStatus())) {
                    List<Date> endDates = statusDateMap.get(taskSnapshot.getStatus());
                    if (CollectionUtils.isNotEmpty(endDates)) {
                        endDates.stream().max(Date::compareTo).ifPresent(date -> {
                            endDate.set(date);
                            vo.setEndDate(DateUtil.toLocalDateTime(date).format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN)));
                        });
                    }
                }
            }

            Long inputTotal = r.getInputTotal();
            Long outputTotal = r.getOutputTotal();

            if (ObjectUtils.anyNull(inputTotal, outputTotal) ||
                    inputTotal == 0 || outputTotal == 0 ||
                    (Objects.isNull(endDate.get())) ||
                    (endDate.get().getTime() > (System.currentTimeMillis() - 60000)) ||
                    TaskDto.STATUS_RUNNING.equals(vo.getStatus())) {
                Long[] values = measurementServiceV2.countEventByTaskRecord(taskId, taskRecordId);
                if (null != values && values.length == 2) {
                    inputTotal = values[0];
                    outputTotal = values[1];

                    Query id = Query.query(Criteria.where("_id").is(taskRecordId));
                    Update set = Update.update("inputTotal", inputTotal).set("outputTotal", outputTotal);
                    CompletableFuture.runAsync(() -> mongoTemplate.updateFirst(id, set, TaskRecord.class));
                }
            }
            vo.setInputTotal(inputTotal);
            vo.setOutputTotal(outputTotal);

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
