package com.tapdata.tm.task.service.impl;

import com.google.common.base.Splitter;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class TaskResetLogServiceImpl implements TaskResetLogService {
    private MongoTemplate mongoTemplate;

    @Autowired
    private TaskService taskService;

    @Value("${task.reset.times: 2}")
    private int resetAllTimes;
    @Value("${task.reset.interval: 30000}")
    private int resetInterval;

    @Autowired
    private StateMachineService stateMachineService;

    public TaskResetLogServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }


    /**
     * @see com.tapdata.tm.statemachine.enums.DataFlowEvent#RENEW_DEL_FAILED
     * @param resetEventDto
     * @param user
     * @return
     */
    @Override
    public TaskResetEventDto save(TaskResetEventDto resetEventDto, UserDetail user) {
         //封装成为日志上报给前端。
        TaskDto taskDto = taskService.checkExistById(new ObjectId(resetEventDto.getTaskId()), user);
        int resetTimes = taskDto.getResetTimes() == null ? 0 : taskDto.getResetTimes();
        resetEventDto.setTime(new Date());
        resetEventDto.setResetTimes(resetTimes);
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

                StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_FAILED, user);
                if (stateMachineResult.isOk()) {
                    Query query = Query.query(Criteria.where("_id").is(taskDto.getId()));
                    Update update = Update.update("last_updated", new Date());
                    taskService.update(query, update);
                }

                resetEventDto.setResetInterval(resetInterval / 1000);
                resetEventDto.setResetAllTimes(resetAllTimes);
                break;
            default:
                break;
        }

        //        推送给前端
//        if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus())) {
//            EditFlushHandler.sendEditFlushMessage(taskDto.getId().toHexString(), resetEventDto, "resetReport");
//        }
        return mongoTemplate.insert(resetEventDto);
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
            criteria.and("level").in(Splitter.on(",").trimResults().splitToList(dto.getGrade()));
        }

        Query query = new Query(criteria);
        List<TaskResetEventDto> taskResetEventDtos = mongoTemplate.find(query, TaskResetEventDto.class);

        //将重置的数据封装成为前端日志需要展示的数据模型
        LinkedHashMap<String, String> nodeMap = new LinkedHashMap<>();
        LinkedList<TaskLogInfoVo> taskLogInfoVos = new LinkedList<>();
        TaskDagCheckLogVo taskDagCheckLogVo = new TaskDagCheckLogVo();
        taskDagCheckLogVo.setOver(false);
        for (TaskResetEventDto taskResetEventDto : taskResetEventDtos) {
            nodeMap.put(taskResetEventDto.getTaskId(), taskResetEventDto.getNodeName());
            TaskLogInfoVo taskLogInfoVo = new TaskLogInfoVo();
            BeanUtils.copyProperties(taskResetEventDto, taskLogInfoVo);
            taskLogInfoVo.setId(taskResetEventDto.getId().toHexString());
            taskLogInfoVos.add(taskLogInfoVo);

            //如果已经存在任务完成，或者任务失败的消息，则说明本次任务的reset操作已经结束，这个over按照前端约定，应该是true， 否则为false
            if (taskResetEventDto.getStatus().equals(TaskResetEventDto.ResetStatusEnum.TASK_FAILED)
                    || taskResetEventDto.getStatus().equals(TaskResetEventDto.ResetStatusEnum.TASK_SUCCEED)) {
                taskDagCheckLogVo.setOver(true);
            }
        }
        taskDagCheckLogVo.setNodes(nodeMap);
        taskDagCheckLogVo.setList(taskLogInfoVos);
        return taskDagCheckLogVo;
    }
}
