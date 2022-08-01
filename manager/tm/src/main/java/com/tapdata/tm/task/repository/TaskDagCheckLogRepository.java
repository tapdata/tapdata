package com.tapdata.tm.task.repository;

import com.tapdata.tm.task.entity.TaskDagCheckLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TaskDagCheckLogRepository extends MongoRepository<TaskDagCheckLog, String> {
}
