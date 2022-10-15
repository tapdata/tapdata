package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.task.bean.TaskResetLogs;
import com.tapdata.tm.task.service.TaskResetLogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class TaskResetLogServiceImpl implements TaskResetLogService {
    private MongoTemplate mongoTemplate;

    public TaskResetLogServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void save(TaskResetLogs taskResetLogs) {
        mongoTemplate.insert(taskResetLogs);
    }

    @Override
    public List<TaskResetLogs> findByTaskId(String taskId) {
        Criteria criteria = Criteria.where("taskId").is(taskId);
        Query query = new Query(criteria);
        query.with(Sort.by("time"));
        return mongoTemplate.find(query, TaskResetLogs.class);
    }

    @Override
    public List<TaskResetLogs> find(Query query) {
        return mongoTemplate.find(query, TaskResetLogs.class);
    }

    @Override
    public void clearLogByTaskId(String taskId) {
        Criteria criteria = Criteria.where("taskId").is(taskId);
        Query query = new Query(criteria);
        mongoTemplate.remove(query, TaskResetLogs.class);
    }
}
