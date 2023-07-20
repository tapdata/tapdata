package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.service.TaskExtendService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskExtendServiceImpl implements TaskExtendService {

    private TaskService taskService;
    private UserService userService;
    private MonitoringLogsService monitoringLogsService;

    @Override
    public void stopTaskByAgentIdAndUserId(String agentId, String userId) {
        Criteria criteria = Criteria.where("agentId").is(agentId).and("user_id").is(userId).and("status").is(TaskDto.STATUS_RUNNING);
        List<TaskDto> all = taskService.findAll(Query.query(criteria));
        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
        all.forEach(taskDto -> {
            CommonUtils.ignoreAnyError(() -> taskService.pause(taskDto, userDetail, false), "TM");

            String msg = "The public agent trial has expired and the task has stopped. Please create a new agent as soon as possible";
            monitoringLogsService.startTaskErrorLog(taskDto, userDetail, msg, Level.WARN);
        });
    }

    @Override
    public void clearFunctionRetry() {
        Query query = Query.query(Criteria.where("functionRetryStatus").is(TaskDto.RETRY_STATUS_RUNNING).and("functionRetryEx").gt(System.currentTimeMillis()));
        Update update = Update.update("functionRetryEx", TaskDto.RETRY_STATUS_NONE);
        taskService.update(query, update);
    }
}
