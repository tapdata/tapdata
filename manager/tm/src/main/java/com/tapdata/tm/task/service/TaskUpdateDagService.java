package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskHistory;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskEntity;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;


@Service
public class TaskUpdateDagService {

    @Autowired
    private MongoTemplate mongoTemplate;

    public void updateDag(TaskDto taskDto, TaskDto oldTask, UserDetail user, boolean saveHistory) {
        updateDag(taskDto.getId(), taskDto.getDag(), saveHistory);
        if (saveHistory) {
            TaskHistory taskHistory = new TaskHistory();
            BeanUtils.copyProperties(oldTask, taskHistory);
            taskHistory.setTaskId(oldTask.getId().toHexString());
            taskHistory.setId(ObjectId.get());

            //保存任务历史
            mongoTemplate.insert(taskHistory, "DDlTaskHistories");
        }
    }

    public void updateDag(ObjectId id, DAG dag) {
        updateDag(id, dag, false);
    }
    public void updateDag(ObjectId id, DAG dag, boolean saveHistory) {
        Criteria criteria = Criteria.where("_id").is(id);
        Update update = Update.update("dag", dag);
        long tmCurrentTime = System.currentTimeMillis();
        if (saveHistory) {
            update.set("tmCurrentTime", tmCurrentTime);
        }
        mongoTemplate.updateFirst(new Query(criteria), update, TaskEntity.class);
    }
}
