package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.task.vo.TaskRecordListVo;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskRecordServiceImpl implements TaskRecordService {
    private MongoTemplate mongoTemplate;

    @Override
    public void createRecord(TaskRecord taskRecord) {
        mongoTemplate.save(taskRecord);
    }

    @Override
    public void updateTaskStatus(SyncTaskStatusDto dto) {
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(dto.getTaskRecordId())));
        Update update = new Update().set("taskSnapshot.status", dto.getTaskStatus());
        mongoTemplate.updateFirst(query, update, TaskRecord.class);
    }

    @Override
    public Page<TaskRecordListVo> queryRecords(String taskId, String offset, Integer limit) {
        Query query = new Query(Criteria.where("taskId").is(taskId));
        long count = mongoTemplate.count(query, TaskRecord.class);
        if (count == 0) {
            return new Page<TaskRecordListVo>(){{setTotal(count);}};
        }

        query.addCriteria(Criteria.where("_id").gt(offset));
        query.limit(limit);
        List<TaskRecord> taskRecords = mongoTemplate.find(query, TaskRecord.class);


        List<TaskRecordListVo> collect = taskRecords.stream().map(r -> {
            TaskRecordListVo vo = new TaskRecordListVo();
            vo.setTaskId(r.getTaskId());
            vo.setTaskRecordId(r.getId().toHexString());
            TaskEntity taskSnapshot = r.getTaskSnapshot();
            vo.setStartDate(taskSnapshot.getStartTime());
            // TODO jiaxin: 2022/8/10 endTime
//            vo.setEndDate(taskSnapshot.getStartTime());
            vo.setStatus(taskSnapshot.getStatus());
            return vo;
        }).collect(Collectors.toList());

        Page<TaskRecordListVo> result = new Page<TaskRecordListVo>();
        result.setItems(collect);
        result.setTotal(count);

        return result;
    }
}
