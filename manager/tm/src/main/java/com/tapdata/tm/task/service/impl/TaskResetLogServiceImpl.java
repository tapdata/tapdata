package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.task.service.TaskResetLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.task.vo.TaskLogInfoVo;
import com.tapdata.tm.ws.handler.EditFlushHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class TaskResetLogServiceImpl implements TaskResetLogService {
    private MongoTemplate mongoTemplate;

    @Autowired
    private TaskService taskService;

    public TaskResetLogServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }


    @Override
    public TaskResetEventDto save(TaskResetEventDto resetEventDto, UserDetail user) {
         //封装成为日志上报给前端。
        TaskDto taskDto = taskService.checkExistById(new ObjectId(resetEventDto.getTaskId()), user);

        resetEventDto.setTime(new Date());
        switch (resetEventDto.getStatus()) {
            case TASK_SUCCEED:
                //根据任务的状态，如果是重置中，则继续重置的操作，如果为删除中，则删除继续删除的操作
                if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus())) {
                    taskService.afterRenew(taskDto, user);
                } else if (TaskDto.STATUS_DELETING.equals(taskDto.getStatus())) {
                    taskService.afterRemove(taskDto, user);
                }
                break;
            case TASK_FAILED:
                //根据任务的状态，如果是重置中，更新成为重置失败，如果为删除中，则删除失败
                if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus())) {
                    taskService.updateStatus(taskDto.getId(), TaskDto.STATUS_RENEW_FAILED);
                } else if (TaskDto.STATUS_DELETING.equals(taskDto.getStatus())) {
                    taskService.updateStatus(taskDto.getId(), TaskDto.STATUS_DELETE_FAILED);
                }
                break;
            default:
                break;
        }

        TaskResetEventDto taskResetEventDto = mongoTemplate.insert(resetEventDto);

        //推送给前端
        if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus())) {
            EditFlushHandler.sendEditFlushMessage(taskDto.getId().toHexString(), resetEventDto, "resetReport");
        }
        return taskResetEventDto;
    }


    @Override
    public List<TaskResetEventDto> find(Query query) {
        return mongoTemplate.find(query, TaskResetEventDto.class);
    }

    @Override
    public void clearLogByTaskId(String taskId) {
        Criteria criteria = Criteria.where("taskId").is(taskId);
        Query query = new Query(criteria);
        mongoTemplate.remove(query, TaskResetEventDto.class);
    }

    @Override
    public TaskDagCheckLogVo getLogs(TaskLogDto dto) {
        Criteria criteria = Criteria.where("taskId").is(dto.getTaskId());
        if (StringUtils.isNotBlank(dto.getNodeId())) {
            criteria.and("nodeId").is(dto.getNodeId());
        }
        if (StringUtils.isNotBlank(dto.getGrade())) {
            criteria.and("level").is(dto.getGrade());
        }

        if (StringUtils.isNotBlank(dto.getKeyword())) {
            criteria.and("level").is(dto.getGrade());
        }

        Query query = new Query(criteria);
        List<TaskResetEventDto> taskResetEventDtos = mongoTemplate.find(query, TaskResetEventDto.class);

        LinkedHashMap<String, String> nodeMap = new LinkedHashMap<>();
        LinkedList<TaskLogInfoVo> taskLogInfoVos = new LinkedList<>();
        for (TaskResetEventDto taskResetEventDto : taskResetEventDtos) {
            nodeMap.put(taskResetEventDto.getTaskId(), taskResetEventDto.getNodeName());
            TaskLogInfoVo taskLogInfoVo = new TaskLogInfoVo();
            BeanUtils.copyProperties(taskResetEventDto, taskLogInfoVo);
            taskLogInfoVo.setId(taskResetEventDto.getId().toHexString());
            taskLogInfoVos.add(taskLogInfoVo);
        }
        TaskDagCheckLogVo taskDagCheckLogVo = new TaskDagCheckLogVo();
        taskDagCheckLogVo.setNodes(nodeMap);
        taskDagCheckLogVo.setList(taskLogInfoVos);
        return taskDagCheckLogVo;
    }
}
